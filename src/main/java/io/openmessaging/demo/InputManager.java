package io.openmessaging.demo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.openmessaging.Message;

public class InputManager {
	private static Logger logger = Logger.getGlobal();

	private static InputManager INSTANCE;

	static AtomicInteger numFetchSegs = new AtomicInteger();
	static AtomicInteger numFetchSuperSegs = new AtomicInteger();
	static AtomicInteger numConsumeSegs = new AtomicInteger();
	static AtomicInteger numConsumeSuperSegs = new AtomicInteger();
	static HashMap<String, AtomicInteger> processedSegmentNumMap;

	private MetaInfo allMetaInfo;

	private String storePath;

	private DiskFetchService[] fetchServices;
	private Thread[] fetchThreads;
	private HashMap<String, Integer> bucketRankMap;

	private PriorityBlockingQueue<DecompressSuperSegReq> decompressReqQueue;
	private DecompressService decompressService;
	private Thread decompressThread;

	/**
	 * bucket queues partition, every msgEncoderService hold a part of bucket
	 * within a specific readBufferQueue.
	 */
	private BlockingQueue<ConsecutiveSegs>[] readConsecutiveSegsQueues;
	private HashMap<String, BlockingQueue<ConsecutiveSegs>> bucketConsecutiveSegsQueueMap;
	private MessageEncoderService[] msgEncoderServices;
	private Thread[] msgEncoderThreads;

	private InputManager() {
		this.storePath = SmartMessageStore.STORE_PATH;

		// init bucketMetas
		allMetaInfo = loadAllMetaInfo(storePath);

		System.out.println(allMetaInfo.queues);
		System.out.println(allMetaInfo.topics);
		processedSegmentNumMap = new HashMap<>(allMetaInfo.queuesSize + allMetaInfo.topicsSize);

		for (String bucket : Config.HACK_BUCKETS) {
			processedSegmentNumMap.put(bucket, new AtomicInteger());
		}

		this.decompressReqQueue = new PriorityBlockingQueue<>(Config.DECOMPRESS_REQUEST_QUEUE_SIZE);
		this.decompressService = new DecompressService();
		this.decompressThread = new Thread(this.decompressService);
		this.decompressThread.start();

		// start Fetch service and message encoder services
		this.fetchServices = new DiskFetchService[Config.NUM_READ_DISK_THREAD];
		this.fetchThreads = new Thread[Config.NUM_READ_DISK_THREAD];
		for (int i = 0; i < Config.NUM_READ_DISK_THREAD; i++) {
			this.fetchServices[i] = new DiskFetchService(i);
			this.fetchThreads[i] = new Thread(fetchServices[i]);
		}

		readConsecutiveSegsQueues = new LinkedBlockingQueue[Config.NUM_ENCODER_MESSAGE_THREAD];
		for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
			readConsecutiveSegsQueues[i] = new LinkedBlockingQueue<>(Config.READ_BUFFER_QUEUE_SIZE);
		}

		// uniform distribution
		bucketRankMap = new HashMap<>(Config.NUM_BUCKETS);
		bucketConsecutiveSegsQueueMap = new HashMap<>(Config.NUM_BUCKETS);
		int tempCnt = 0;
		for (String bucket : Config.HACK_BUCKETS) {
			bucketConsecutiveSegsQueueMap.put(bucket,
					readConsecutiveSegsQueues[tempCnt++ % Config.NUM_ENCODER_MESSAGE_THREAD]);
			bucketRankMap.put(bucket, tempCnt++ % Config.NUM_READ_DISK_THREAD);
		}

		msgEncoderServices = new MessageEncoderService[Config.NUM_ENCODER_MESSAGE_THREAD];
		msgEncoderThreads = new Thread[Config.NUM_ENCODER_MESSAGE_THREAD];

		for (int i = 0; i < Config.NUM_READ_DISK_THREAD; i++) {
			this.fetchThreads[i].start();
		}

	}

	public static InputManager getInstance() {
		if (INSTANCE == null)
			INSTANCE = new InputManager();
		return INSTANCE;
	}

	public void startPullService(HashMap<String, ArrayList<BlockingQueue<MessagePool>>> bucketBindingMsgQueuesMap) {
		logger.info("Start " + Config.NUM_ENCODER_MESSAGE_THREAD + " messge encoder services");
		for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
			msgEncoderServices[i] = new MessageEncoderService(readConsecutiveSegsQueues[i], bucketBindingMsgQueuesMap);
			msgEncoderThreads[i] = new Thread(msgEncoderServices[i]);
			msgEncoderThreads[i].start();
		}
	}

	public static MetaInfo loadAllMetaInfo(String store_dir) {
		logger.info("Start load the meta info");
		MetaInfo allMetaInfo = null;

		long start = System.currentTimeMillis();
		RandomAccessFile memoryMappedFile = null;
		MappedByteBuffer buffer = null;
		Path p = Paths.get(store_dir, SmartMessageStore.META_FILE);
		String filename = p.toString();
		long fileSize = 0;
		try {
			memoryMappedFile = new RandomAccessFile(filename, "r");
			fileSize = memoryMappedFile.length();
			buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

		} catch (IOException e) {
			e.printStackTrace();
		}

		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new ByteBufferInputStream(buffer));
			allMetaInfo = (MetaInfo) ois.readObject();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		try {
			ois.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			ois = null;
		}

		// assert
		if (allMetaInfo.queuesSize + allMetaInfo.topicsSize != allMetaInfo.size()) {
			logger.severe(String.format("Inconsistent: queuesSize = %d, topicSize = %d, bucketMetas.size() = %d",
					allMetaInfo.queuesSize, allMetaInfo.topicsSize, allMetaInfo.size()));
		}

		try {
			memoryMappedFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		logger.info(String.format("Read meta from %s cost %d ms, size %d bytes", filename, end - start, fileSize));
		logger.info(allMetaInfo.toString());
		return allMetaInfo;
	}

	public MetaInfo getAllMetaInfo() {
		return allMetaInfo;
	}

	static CountDownLatch diskFetchlatch = new CountDownLatch(Config.NUM_READ_DISK_THREAD);

	class DiskFetchService implements Runnable {

		int rank;

		DiskFetchService(int rank) {
			this.rank = rank;
		}

		@Override
		public void run() {
			HashMap<Integer, FileSuperSeg> fileSuperSegMap = allMetaInfo.getFileSuperSegMap();
			for (int fileId = rank; fileId < allMetaInfo.numDataFiles; fileId += Config.NUM_READ_DISK_THREAD) {
				FileSuperSeg fileSuperSeg = fileSuperSegMap.get(fileId);
				long start = System.currentTimeMillis();
				Path p = Paths.get(storePath, fileId + ".data");
				String filename = p.toString();

				int numSegsInSuperSeg = fileSuperSeg.numSegsInSuperSeg;
				// long readFileSize = numSegsInSuperSeg * Segment.CAPACITY;

				RandomAccessFile memoryMappedFile = null;
				MappedByteBuffer buffer = null;
				try {

					memoryMappedFile = new RandomAccessFile(filename, "r");
					buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
							fileSuperSeg.compressedSize);
					memoryMappedFile.close();
					// load the segments into the physical memory
					// TODO, when to put the buffer, avoiding the page swap.
					buffer.load();
					decompressReqQueue.put(new DecompressSuperSegReq(fileId, buffer, fileSuperSeg.numSegsInSuperSeg,
							fileSuperSeg.compressedSize));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					memoryMappedFile = null;
				}

				// /**
				// * readFileSize isn't same with file size necessarily.
				// */
				// long actualFileSize = new File(filename).length();
				// if (readFileSize > actualFileSize) {
				// logger.warning(
				// "read size (" + readFileSize + ") exceeds actual file size ("
				// + actualFileSize + ")");
				// }
				// if (readFileSize < actualFileSize) {
				// logger.warning(
				// "read size (" + readFileSize + ") is less than actual file
				// size (" + actualFileSize + ")");
				// }

				numFetchSegs.addAndGet(numSegsInSuperSeg);

				long end = System.currentTimeMillis();

				logger.info(String.format("(%dth superseg) Read super-segment data from %s cost %d ms, %d bytes",
						numFetchSuperSegs.get(), filename, end - start, fileSuperSeg.compressedSize));
			}

			diskFetchlatch.countDown();
			try {
				diskFetchlatch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

			if (rank == 0) {
				logger.info("Read all the files, emit finish signal");
				decompressReqQueue.put(DecompressSuperSegReq.NULL);
			}
		}
	}

	class DecompressService implements Runnable {
		@Override
		public void run() {
			HashMap<Integer, FileSuperSeg> fileSuperSegMap = allMetaInfo.getFileSuperSegMap();
			while (true) {
				DecompressSuperSegReq req = null;
				try {
					req = decompressReqQueue.take();
					if (req != DecompressSuperSegReq.NULL) {
						DecompressedSuperSegment dss = new DecompressedSuperSegment(req.buffer, req.numSegsInSuperSeg,
								req.compressedSize);

						byte[] superSegmentBinary = dss.decompress();
						logger.info("hehe fileid " + req.fileId);

						FileSuperSeg fileSuperSeg = fileSuperSegMap.get(req.fileId);
						int segCursor = 0;
						for (SequentialSegs sss : fileSuperSeg.sequentialSegs) {
						logger.info("hehe bucket " + sss.bucket);
							String bucket = sss.bucket;
							// TODO multi-service
							// distinguish the bucket
							BlockingQueue<ConsecutiveSegs> readConsecutiveSegsQueue = bucketConsecutiveSegsQueueMap
									.get(bucket);
							if (readConsecutiveSegsQueue == null) {
								logger.severe("lack corresponding readBufferQueue " + bucket);
							}
							readConsecutiveSegsQueue.put(
									new ConsecutiveSegs(req.numSegsInSuperSeg, bucket, superSegmentBinary, segCursor));
							segCursor += sss.numSegs;
						}
					} else {
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
				try {
					readConsecutiveSegsQueues[i].put(ConsecutiveSegs.NULL);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	class MessageEncoderService implements Runnable {
		HashMap<String, ArrayList<BlockingQueue<MessagePool>>> bucketBindingMsgQueuesMap;
		BlockingQueue<ConsecutiveSegs> readConsecutiveSegsQueue;

		public MessageEncoderService(BlockingQueue<ConsecutiveSegs> readConsecutiveSegsQueue,
				HashMap<String, ArrayList<BlockingQueue<MessagePool>>> bucketBindingMsgQueuesMap) {
			this.readConsecutiveSegsQueue = readConsecutiveSegsQueue;
			this.bucketBindingMsgQueuesMap = bucketBindingMsgQueuesMap;
		}

		@Override
		public void run() {
			while (true) {
				ConsecutiveSegs sc = null;
				try {
					sc = readConsecutiveSegsQueue.take();
					if (sc == ConsecutiveSegs.NULL) {
						break;
					} else {
						processSequentialSegments(sc);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("Encoder service " + Thread.currentThread().toString() + " ended.");
		}

		/**
		 * process seqential segs with the same bucket
		 * 
		 * @param buffer
		 * @param offsetInSuperSegment
		 * @param segLength
		 */
		private void processSequentialSegments(ConsecutiveSegs sc) {
			for (int i = 0; i < sc.numSegs; i++) {
				ReadableSegment readSegment = ReadableSegment.wrap(sc.buff, i * Segment.CAPACITY, Segment.CAPACITY);
				String b = readSegment.bucket;
				if (!sc.bucket.equals(b)) {
					logger.warning("Error " + b + " != " + sc.bucket);
				}
				int processedSegmentNum = processedSegmentNumMap.get(b).incrementAndGet();
				ArrayList<BlockingQueue<MessagePool>> queueList = bucketBindingMsgQueuesMap.get(b);

				MessagePool pool = null;
				Message msg = null;
				try {
					pool = new MessagePool(Config.MAX_MESSAGE_POOL_CAPACITY);
					while (true) {
						msg = readSegment.read();
						if (!pool.addMessageIfRemain(msg)) {
							// pool full
							for (BlockingQueue<MessagePool> queue : queueList)
								queue.put(pool);
							pool = new MessagePool(Config.MAX_MESSAGE_POOL_CAPACITY);
						}
					}
				} catch (SegmentEmptyException e) {
					// pool not full, not empty
					if (pool.limit > 0) {
						for (BlockingQueue<MessagePool> queue : queueList)
							try {
								queue.put(pool);
							} catch (InterruptedException e2) {
								e2.printStackTrace();
							}
					}

					// send the nullMessage as a signal
					if (processedSegmentNum >= allMetaInfo.get(b).numSegs) {
						for (BlockingQueue<MessagePool> queue : queueList)
							try {
								queue.put(MessagePool.nullMessagePool);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

/**
 * Consecutive Segs with the same bucket
 * 
 * @author andrew
 *
 */
class ConsecutiveSegs {

	public final static ConsecutiveSegs NULL = new ConsecutiveSegs();
	int numSegs;
	String bucket;
	byte[] buff;
	int offsetSegUnit;

	private ConsecutiveSegs() {

	}

	public ConsecutiveSegs(int numSegs, String bucket, byte[] buff, int offsetSegUnit) {
		super();
		this.numSegs = numSegs;
		this.bucket = bucket;
		this.buff = buff;
		this.offsetSegUnit = offsetSegUnit;
	}

}

class DecompressSuperSegReq implements Comparable<DecompressSuperSegReq> {
	int fileId;
	int numSegsInSuperSeg;
	ByteBuffer buffer;
	long compressedSize;

	public static final DecompressSuperSegReq NULL = new DecompressSuperSegReq();
	static {
		NULL.fileId = Integer.MAX_VALUE;
	}

	DecompressSuperSegReq() {

	}

	public DecompressSuperSegReq(int fileId, MappedByteBuffer buffer, int numSegsInSuperSeg, long compressedSize) {
		this.fileId = fileId;
		this.buffer = buffer;
		this.numSegsInSuperSeg = numSegsInSuperSeg;
		this.compressedSize = compressedSize;
	}

	@Override
	public int compareTo(DecompressSuperSegReq o) {
		return this.fileId - o.fileId;
	}
}

class MessagePool {
	final static MessagePool nullMessagePool = new MessagePool();
	Message[] msgs;
	int limit;

	MessagePool(int size) {
		msgs = new Message[size];
	}

	/**
	 * now there is a space
	 * 
	 * @param msg
	 * @return true if now is not full. false if now is full.
	 */
	public boolean addMessageIfRemain(Message msg) {
		if (limit >= msgs.length)
			return false;
		msgs[limit++] = msg;
		if (limit == msgs.length)
			return false;
		else
			return true;
	}

	MessagePool() {

	}
}
