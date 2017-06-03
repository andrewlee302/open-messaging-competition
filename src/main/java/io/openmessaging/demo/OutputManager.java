package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
				BucketWriteBox.occurContentNotEnough.get(), BucketWriteBox.occurMetaNotEnough.get()));

	}

	public void writeSegment(BlockingQueue<WritableSegment> callbackQueue, String bucket, WritableSegment seg,
			int index) {
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
		// same with numDataFiles
		allMetaInfo.setNumSuperSegs(persistencyService.numSuperSegs);
		allMetaInfo.setNumTotalSegs(persistencyService.numTotalSegs);
		allMetaInfo.setNumMetaRecord(persistencyService.numMetaRecord);
		allMetaInfo.setFileSuperSegMap(persistencyService.fileSuperSegMap);
		allMetaInfo.setSequentialOccurs(persistencyService.sequentialOccurs);

		long start = System.currentTimeMillis();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(2 << 20); // 1Mb
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
		// logger.info(String.format("Write meta to %s cost %d ms, size %d
		// bytes, writeRate: %.3f m/s", filename,
		// end - start, meta.length, ((double) totalWriteDiskSize) / (1 << 20) /
		// totalWriteDiskCost * 1000));
	}

	class PersistencyService implements Runnable {

		int fileId = 0; // from 0
		int numTotalSegs = 0;
		int numMetaRecord = 0;
		int numSuperSegs = 0;
		int sequentialOccurs = 0;

		HashMap<Integer, FileSuperSeg> fileSuperSegMap;

		public PersistencyService() {
			this.fileSuperSegMap = new HashMap<>();
		}

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
				// logger.info(String.format("Catch %d write reqs",
				// reqs.size()));
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
			int reqSize = reqs.size();
			if (reqSize == 0) {
				return;
			}
			numTotalSegs += reqSize;
			int fileSize = reqSize * Segment.CAPACITY;
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
			FileSuperSeg fileSuperSeg = new FileSuperSeg(reqSize);
			fileSuperSegMap.put(superSegFileId, fileSuperSeg);

			// keep the order of one bucket for combination
			Collections.sort(reqs);

			WriteRequest preReq = reqs.get(0);
			String preBucket = preReq.bucket;
			int preIndex = preReq.index;
			int offset = 0, numSegs = 0;
			for (int i = 0; i < reqs.size(); i++) {
				WriteRequest req = reqs.get(i);
				// allMetaInfo.addBucketInfo(req.bucket);
				byte[] data = req.seg.assemble();
				buffer.put(data);
				try {
					req.seg.clear();
					req.callbackQueue.put(req.seg);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (!req.bucket.equals(preBucket)) {
					sequentialOccurs++;
					fileSuperSeg.sequentialSegs.add(new SequentialSegs(preBucket, numSegs));
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
					// offset = i;
					numSegs = 1;
				} else {
					numSegs++;
				}
			}
			// last bucket group of segments
			sequentialOccurs++;
			fileSuperSeg.sequentialSegs.add(new SequentialSegs(preBucket, numSegs));

			BucketMeta meta = allMetaInfo.get(preBucket);
			if (meta == null) {
				meta = new BucketMeta();
				allMetaInfo.put(preBucket, meta);
			}
			numMetaRecord++;
			meta.addMetaRecord(preIndex, superSegFileId, offset, numSegs);
			meta.addNumSegs(numSegs);

			long end = System.currentTimeMillis();
			logger.info(String.format("(%dth) Write data (%d %dth segs) to %s cost %d ms, size %d bytes",
					++numSuperSegs, reqs.size(), numTotalSegs, filename, end - start, fileSize));
		}
	}
}

class NullWriteRequest extends WriteRequest {

	public NullWriteRequest() {
		super();
	}

}

class WriteRequest implements Comparable<WriteRequest> {
	BlockingQueue<WritableSegment> callbackQueue;
	String bucket;
	WritableSegment seg;
	int index;

	WriteRequest() {

	}

	public WriteRequest(BlockingQueue<WritableSegment> callbackQueue, String bucket, WritableSegment seg, int index) {
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
