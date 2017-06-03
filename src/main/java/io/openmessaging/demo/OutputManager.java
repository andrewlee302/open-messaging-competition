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
	private BlockingQueue<CompressRequest> compressReqQueue;
	private BlockingQueue<PersistSuperSegRequest> persistReqQueue;

	private CompressService compressService;
	private Thread compressThread;

	private PersistencyService persistencyService;
	private Thread persistencyThread;
	private String storePath;

	private OutputManager() {
		this.storePath = SmartMessageStore.STORE_PATH;
		this.compressReqQueue = new LinkedBlockingQueue<>(Config.COMPRESS_REQUEST_QUEUE_SIZE);
		this.persistReqQueue = new LinkedBlockingQueue<>(Config.PERSIST_REQUEST_QUEUE_SIZE);

		this.allMetaInfo = new MetaInfo(Config.NUM_BUCKETS);

		// start compress and persistency service
		this.compressService = new CompressService();
		this.compressThread = new Thread(compressService);
		this.compressThread.start();

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
			compressReqQueue.put(CompressRequest.NULL);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			compressThread.join();
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

	public void sendToCompressReqQueue(BlockingQueue<WritableSegment> callbackQueue, String bucket, WritableSegment seg,
			int index) {
		try {
			compressReqQueue.put(new CompressRequest(callbackQueue, bucket, seg, index));
			if (bucket != seg.bucket) {
				logger.warning("why ");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

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
		allMetaInfo.setNumDataFiles(compressService.fileId);
		// same with numDataFiles
		allMetaInfo.setNumSuperSegs(compressService.numSuperSegs);
		allMetaInfo.setNumTotalSegs(compressService.numTotalSegs);
		allMetaInfo.setNumMetaRecord(compressService.numMetaRecord);
		allMetaInfo.setFileSuperSegMap(compressService.fileSuperSegMap);
		allMetaInfo.setSequentialOccurs(compressService.sequentialOccurs);

		ByteArrayOutputStream baos = new ByteArrayOutputStream(1 << 22); // 4Mb
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

		logger.info(String.format("Write meta to %s, size %d bytes", filename, meta.length));
	}

	class CompressService implements Runnable {

		int fileId = 0; // from 0
		int numTotalSegs = 0;
		int numMetaRecord = 0;
		int numSuperSegs = 0;
		int sequentialOccurs = 0;

		byte[] compressBuff;

		HashMap<Integer, FileSuperSeg> fileSuperSegMap;

		public CompressService() {
			this.fileSuperSegMap = new HashMap<>();
			this.compressBuff = new byte[Config.SEGMENT_SIZE * Config.REQ_BATCH_COUNT_THRESHOLD]; // 8M
		}

		@Override
		public void run() {
			final int reqBatchCountThreshold = Config.REQ_BATCH_COUNT_THRESHOLD;
			final long bathThreasholdTime = Config.REQ_WAIT_TIME_THRESHOLD; // ms
			boolean isEnd = false;
			while (!isEnd) {
				ArrayList<CompressRequest> reqs = new ArrayList<>(reqBatchCountThreshold);
				for (int i = 0; i < reqBatchCountThreshold; i++) {
					CompressRequest req = null;
					try {
						req = compressReqQueue.poll(bathThreasholdTime, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (req == null) {
						break;
					} else {
						if (req == CompressRequest.NULL) {
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
				compressSuperSegment(reqs);
				// logger.info(String.format("Catch %d write reqs",
				// reqs.size()));
				reqs.clear();
			}
			try {
				persistReqQueue.put(PersistSuperSegRequest.NULL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * One super segment consists more than one groups (with the same
		 * bucket) of segments. Construction:
		 * 
		 * @param reqs
		 */
		private void compressSuperSegment(List<CompressRequest> reqs) {
			int reqSize = reqs.size();
			if (reqSize == 0) {
				return;
			}
			CompressedSuperSegment css = new CompressedSuperSegment(compressBuff);
			numTotalSegs += reqSize;
			long start = System.currentTimeMillis();

			int superSegFileId = fileId++;
			FileSuperSeg fileSuperSeg = new FileSuperSeg(reqSize);
			fileSuperSegMap.put(superSegFileId, fileSuperSeg);

			// keep the order of one bucket for combination
			Collections.sort(reqs);

			logger.info("hehe fileid " + superSegFileId);
			CompressRequest preReq = reqs.get(0);
			String preBucket = preReq.bucket;
			int preIndex = preReq.index;
			int offset = 0, numSegsTmp = 0;
			for (int i = 0; i < reqs.size(); i++) {
				CompressRequest req = reqs.get(i);
				// allMetaInfo.addBucketInfo(req.bucket);
				css.append(req.seg);

				try {
					req.seg.clear();
					req.callbackQueue.put(req.seg);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (!req.bucket.equals(preBucket)) {
					sequentialOccurs++;
					logger.info("hehe bucket " + preBucket);
					fileSuperSeg.sequentialSegs.add(new SequentialSegs(preBucket, numSegsTmp));
					BucketMeta meta = allMetaInfo.get(preBucket);
					if (meta == null) {
						meta = new BucketMeta();
						allMetaInfo.put(preBucket, meta);
					}
					numMetaRecord++;
					meta.addMetaRecord(preIndex, superSegFileId, offset, numSegsTmp);
					meta.addNumSegs(numSegsTmp);

					preBucket = req.bucket;
					preIndex = req.index;
					offset = i;
					numSegsTmp = 1;
				} else {
					numSegsTmp++;
				}
			}
			// last bucket group of segments
			sequentialOccurs++;
			logger.info("hehe bucket " + preBucket);
			fileSuperSeg.sequentialSegs.add(new SequentialSegs(preBucket, numSegsTmp));

			BucketMeta meta = allMetaInfo.get(preBucket);
			if (meta == null) {
				meta = new BucketMeta();
				allMetaInfo.put(preBucket, meta);
			}
			numMetaRecord++;
			meta.addMetaRecord(preIndex, superSegFileId, offset, numSegsTmp);
			meta.addNumSegs(numSegsTmp);

			byte[] compressData = css.getCompressedData();
			fileSuperSeg.compressedSize = compressData.length;
			try {
				persistReqQueue.put(new PersistSuperSegRequest(compressData, superSegFileId, reqSize));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			long end = System.currentTimeMillis();
			logger.info(String.format("(%dth) Compress data (%d->%d) cost %d ms", ++numSuperSegs,
					reqSize * Config.SEGMENT_SIZE, compressData.length, end - start));
		}
	}

	class PersistencyService implements Runnable {

		int numPersistSuperSeg = 0;

		HashMap<Integer, FileSuperSeg> fileSuperSegMap;

		public PersistencyService() {
			this.fileSuperSegMap = new HashMap<>();
		}

		@Override
		public void run() {
			while (true) {
				PersistSuperSegRequest req = null;
				try {
					req = persistReqQueue.take();
					if (req == PersistSuperSegRequest.NULL) {
						logger.info("Receive the end signal");
						break;
					} else {
						persistSuperSegment(req);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * One super segment consists more than one groups (with the same
		 * bucket) of segments. Construction:
		 * 
		 * @param reqs
		 */
		private void persistSuperSegment(PersistSuperSegRequest req) {
			int fileSize = req.compressedData.length;
			long start = System.currentTimeMillis();
			RandomAccessFile memoryMappedFile = null;
			MappedByteBuffer buffer = null;
			int superSegFileId = req.fileId;
			Path p = Paths.get(storePath, superSegFileId + ".data");
			String filename = p.toString();
			try {
				memoryMappedFile = new RandomAccessFile(filename, "rw");
				buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
				memoryMappedFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			buffer.put(req.compressedData);

			long end = System.currentTimeMillis();
			logger.info(String.format("(%dth) Persist data (%d) to %s cost %d ms", ++numPersistSuperSeg, fileSize,
					filename, end - start));
		}
	}
}

class PersistSuperSegRequest {

	public static final PersistSuperSegRequest NULL = new PersistSuperSegRequest();
	int numSegs;
	byte[] compressedData;
	int fileId;

	PersistSuperSegRequest() {

	}

	public PersistSuperSegRequest(byte[] compressedData, int fileId, int numSegs) {
		super();
		this.compressedData = compressedData;
		this.fileId = fileId;
		this.numSegs = numSegs;
	}

}

class CompressRequest implements Comparable<CompressRequest> {
	public final static CompressRequest NULL = new CompressRequest();

	BlockingQueue<WritableSegment> callbackQueue;
	String bucket;
	WritableSegment seg;
	int index;

	CompressRequest() {

	}

	public CompressRequest(BlockingQueue<WritableSegment> callbackQueue, String bucket, WritableSegment seg,
			int index) {
		super();
		this.callbackQueue = callbackQueue;
		this.bucket = bucket;
		this.seg = seg;
		this.index = index;
	}

	@Override
	public int compareTo(CompressRequest o) {
		int flag = bucket.compareTo(o.bucket);
		return flag;
	}
}