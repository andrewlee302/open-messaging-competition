package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.swing.ButtonGroup;

public class OutputManager {
	private static Logger logger = Logger.getGlobal();

	private static OutputManager INSTANCE;

	private MetaInfo allMetaInfo;
	private BlockingQueue<WriteRequest> writeReqQueue;
	private PersistencyService persistencyService;
	private Thread persistencyThread;
	private String storePath;

	private OutputManager() {
		this.storePath = SmartMessageStore.STORE_PATH;
		this.writeReqQueue = new LinkedBlockingQueue<>(Config.WRITE_REQUEST_QUEUE_SIZE);

		// start Persistency Service
		this.allMetaInfo = new MetaInfo(Config.NUM_BUCKETS);
		this.persistencyService = new PersistencyService();
		this.persistencyThread = new Thread(persistencyService);
		this.persistencyThread.start();
	}

	public static OutputManager getInstance() {
		if (INSTANCE == null)
			INSTANCE = new OutputManager();
		return INSTANCE;
	}

	/**
	 * wait all segments persist in the disk or page cache, then persist the all
	 * meta info
	 * 
	 * @param topics
	 * @param queues
	 */
	public void flush(Set<String> queues, Set<String> topics) {
		try {
			writeReqQueue.put(new NullWriteRequest());
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			persistencyThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		updateAndPersistMetaInfo(queues, topics);

		printDebugInfo();
	}

	private void printDebugInfo() {
		logger.info(allMetaInfo.toString());
		logger.info(String.format("occurContentNotEnough = %d, occurMetaNotEnough = %d",
				WriteSegmentQueue.occurContentNotEnough.get(), WriteSegmentQueue.occurMetaNotEnough.get()));

	}

	public void writeSegment(WriteSegmentQueue callbackQueue, String bucket, WritableSegment seg, int index) {
		WriteRequest wr = new WriteRequest(callbackQueue, bucket, seg, index);
		try {
			writeReqQueue.put(wr);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	long totalWriteDiskCost = 0; // ms
	long totalWriteDiskSize = 0; // bytes

	// TODO
	// optimization: directly copy to mapped memory
	/**
	 * 
	 * @param queues
	 * @param topics
	 */
	public void updateAndPersistMetaInfo(Set<String> queues, Set<String> topics) {
		int queuesSize = queues.size();
		int topicsSize = topics.size();
		allMetaInfo.setQueuesSize(queuesSize);
		allMetaInfo.setTopicsSize(topicsSize);
		allMetaInfo.setQueues(queues);
		allMetaInfo.setTopics(topics);
		allMetaInfo.setNumDataFiles(persistencyService.fileId);
		allMetaInfo.setNumTotalSegs(persistencyService.numTotalSegs);
		allMetaInfo.setNumMetaRecord(persistencyService.numMetaRecord);

		long start = System.currentTimeMillis();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(1 << 20); // 1Mb
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(baos);
			oos.writeObject(allMetaInfo);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		byte[] meta = baos.toByteArray();

		RandomAccessFile memoryMappedFile = null;
		MappedByteBuffer buffer = null;
		Path p = Paths.get(storePath, SmartMessageStore.META_FILE);
		String filename = p.toString();
		try {
			memoryMappedFile = new RandomAccessFile(filename, "rw");
			buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, meta.length);
		} catch (IOException e) {
			e.printStackTrace();
		}

		buffer.put(meta);

		try {
			memoryMappedFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		totalWriteDiskCost += (end - start);
		totalWriteDiskSize += meta.length;
		logger.info(String.format("Write meta to %s cost %d ms, size %d bytes, writeRate: %.3f m/s", filename,
				end - start, meta.length, ((double) totalWriteDiskSize) / (1 << 20) / totalWriteDiskCost * 1000));
	}

	class PersistencyService implements Runnable {

		int fileId = 0; // from 0
		int numTotalSegs = 0;
		int numMetaRecord = 0;

		int persistCnt = 0;

		@Override
		public void run() {
			final int reqBatchCountThreshold = Config.REQ_BATCH_COUNT_THRESHOLD;
			final long bathThreasholdTime = Config.REQ_WAIT_TIME_THRESHOLD; // ms
			boolean isEnd = false;
			while (!isEnd) {
				ArrayList<WriteRequest> reqs = new ArrayList<>(reqBatchCountThreshold);
				for (int i = 0; i < reqBatchCountThreshold; i++) {
					WriteRequest req = null;
					try {
						req = writeReqQueue.poll(bathThreasholdTime, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (req == null) {
						break;
					} else {
						if (req instanceof NullWriteRequest) {
							logger.info("Receive the end signal");
							isEnd = true;
							break;
						} else {
							reqs.add(req);
						}
					}
				}
				if (reqs.size() == 0)
					continue;
				persistSuperSegment(reqs);
				logger.info(String.format("Catch %d write reqs", reqs.size()));
				reqs.clear();
			}
		}

		/**
		 * One super segment consists more than one groups (with the same
		 * bucket) of segments. Construction:
		 * 
		 * @param reqs
		 */
		private void persistSuperSegment(List<WriteRequest> reqs) {
			numTotalSegs += reqs.size();
			int fileSize = reqs.size() * Segment.CAPACITY;
			long start = System.currentTimeMillis();
			RandomAccessFile memoryMappedFile = null;
			MappedByteBuffer buffer = null;
			int superSegFileId = fileId++;
			Path p = Paths.get(storePath, superSegFileId + ".data");
			String filename = p.toString();
			try {
				memoryMappedFile = new RandomAccessFile(filename, "rw");
				buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
				memoryMappedFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// keep the order of one bucket for combination
			Collections.sort(reqs);

			WriteRequest preReq = reqs.get(0);
			String preBucket = preReq.bucket;
			int preIndex = preReq.index;
			int offset = 0, numSegs = 0;
			for (int i = 0; i < reqs.size(); i++) {
				WriteRequest req = reqs.get(i);
				allMetaInfo.addBucketInfo(req.bucket);
				byte[] data = req.seg.assemble();
				buffer.put(data);
				try {
					req.seg.clear();
					req.callbackQueue.put(req.seg);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (req.bucket != preBucket) {
					BucketMeta meta = allMetaInfo.get(preBucket);
					if (meta == null) {
						meta = new BucketMeta();
						allMetaInfo.put(preBucket, meta);
					}
					numMetaRecord++;
					meta.addMetaRecord(preIndex, superSegFileId, offset, numSegs);
					meta.addNumSegs(numSegs);

					preBucket = req.bucket;
					preIndex = req.index;
					offset = i;
					numSegs = 1;
				} else {
					numSegs++;
				}
			}
			// last bucket group of segments
			BucketMeta meta = allMetaInfo.get(preBucket);
			if (meta == null) {
				meta = new BucketMeta();
				allMetaInfo.put(preBucket, meta);
			}
			numMetaRecord++;
			meta.addMetaRecord(preIndex, superSegFileId, offset, numSegs);
			meta.addNumSegs(numSegs);

			long end = System.currentTimeMillis();
			logger.info(String.format("(%dth) Write data (%d %dth segs) to %s cost %d ms, size %d bytes", ++persistCnt,
					reqs.size(), numTotalSegs, filename, end - start, fileSize));
		}
	}
}

// all meta info
class MetaInfo extends HashMap<String, BucketMeta> {
	private static final long serialVersionUID = 8853036030285630462L;

	int queuesSize;
	int topicsSize;
	Set<String> queues, topics;

	/**
	 * the number of segment groups, each of which consists consecutive segments
	 * within the same bucket.
	 */
	int numMetaRecord;

	int numTotalSegs;
	int numTotalMsgs;
	int numDataFiles;
	int numSuperSegs;

	/**
	 * the bucket partition values of sequential segments in the data store. we
	 * can partition the segments of the same bucket to the same encoder service
	 * for the message order. Use byte of its hashcode.
	 */
	ArrayList<Byte> bucketParts = new ArrayList<>(65536);
	transient int cursorOfBucketParts = 0;

	@Override
	public String toString() {
		StringBuffer sbq = new StringBuffer();
		for (String q : queues) {
			sbq.append(q);
			sbq.append(", ");
		}

		StringBuffer sbt = new StringBuffer();
		for (String t : topics) {
			sbt.append(t);
			sbt.append(", ");
		}
		return String.format(
				"queueSize = %d, topicsSize = %d, numMetaRecord = %d, numTotalSegs = %d, numTotalMsgs = %d, numDataFiles = %d, numSuperSegs = %d, queues = %s, topics = %s",
				queuesSize, topicsSize, numMetaRecord, numTotalSegs, numTotalMsgs, numDataFiles, numSuperSegs,
				sbq.toString(), sbt.toString());

	}

	public void addBucketInfo(String bucket) {
		bucketParts.add((byte) bucket.hashCode());
	}

	public Byte getNextBucketPart() {
		if (cursorOfBucketParts < bucketParts.size()) {
			return bucketParts.get(cursorOfBucketParts++);
		} else {
			return null;
		}
	}

	public void setBucketParts(ArrayList<Byte> bucketParts) {
		this.bucketParts = bucketParts;
	}

	public int getNumTotalSegs() {
		return numTotalSegs;
	}

	public void setNumTotalSegs(int numTotalSegs) {
		this.numTotalSegs = numTotalSegs;
	}

	public Set<String> getQueues() {
		return queues;
	}

	public void setQueues(Set<String> queues) {
		this.queues = queues;
	}

	public Set<String> getTopics() {
		return topics;
	}

	public void setTopics(Set<String> topics) {
		this.topics = topics;
	}

	public MetaInfo(int numBuckets) {
		super(numBuckets);
	}

	public int getQueuesSize() {
		return queuesSize;
	}

	public void setQueuesSize(int queuesSize) {
		this.queuesSize = queuesSize;
	}

	public int getTopicsSize() {
		return topicsSize;
	}

	public void setTopicsSize(int topicsSize) {
		this.topicsSize = topicsSize;
	}

	public int getNumTotalMsgs() {
		return numTotalMsgs;
	}

	public void setNumTotalMsgs(int numTotalMsgs) {
		this.numTotalMsgs = numTotalMsgs;
	}

	public int getNumDataFiles() {
		return numDataFiles;
	}

	public void setNumDataFiles(int numDataFiles) {
		this.numDataFiles = numDataFiles;
	}

	public int getNumSuperSegs() {
		return numSuperSegs;
	}

	public void setNumSuperSegs(int numSuperSegs) {
		this.numSuperSegs = numSuperSegs;
	}

	public int getNumMetaRecord() {
		return numMetaRecord;
	}

	public void setNumMetaRecord(int numMetaRecord) {
		this.numMetaRecord = numMetaRecord;
	}
}

// One bucket, one segment meta
class BucketMeta implements Serializable {

	private static final long serialVersionUID = -867852685475432061L;

	int numSegs;

	/**
	 * index is the sequential number of message in the bucket. file is ID of
	 * the data file. offset is the segment offset in the file. Unit is one
	 * segment. length is the number of segments. |index, file, offset, length |
	 * index, file, offset, length | ... |
	 */
	ArrayList<Integer> content;
	public transient final static int META_INT_UNIT_SIZE = 4;

	BucketMeta() {
		content = new ArrayList<>(40 * META_INT_UNIT_SIZE);
	}

	/**
	 * Append the meta info of the bucket in one file
	 * 
	 * @param index
	 *            message index
	 * @param fileId
	 *            id of the disk file
	 * @param offset
	 *            segment unit offset in the file
	 * @param length
	 *            the number of segments
	 */
	public void addMetaRecord(int index, int fileId, int offset, int length) {
		content.add(index);
		content.add(fileId);
		content.add(offset);
		content.add(length);
	}

	public ArrayList<Integer> getContent() {
		return content;
	}

	public void setContent(ArrayList<Integer> content) {
		this.content = content;
	}

	public int getNumSegs() {
		return numSegs;
	}

	public int addNumSegs(int delta) {
		this.numSegs += delta;
		return this.numSegs;
	}

	public void setNumSegs(int numSegs) {
		this.numSegs = numSegs;
	}
}

class NullWriteRequest extends WriteRequest {

	public NullWriteRequest() {
		super();
	}

}

class WriteRequest implements Comparable<WriteRequest> {
	WriteSegmentQueue callbackQueue;
	String bucket;
	WritableSegment seg;
	int index;

	WriteRequest() {

	}

	public WriteRequest(WriteSegmentQueue callbackQueue, String bucket, WritableSegment seg, int index) {
		super();
		this.callbackQueue = callbackQueue;
		this.bucket = bucket;
		this.seg = seg;
		this.index = index;
	}

	@Override
	public int compareTo(WriteRequest o) {
		int flag = bucket.compareTo(o.bucket);
		return flag;
		// if (flag == 0)
		// return (index - o.index);
		// else
		// return flag;
	}
}
