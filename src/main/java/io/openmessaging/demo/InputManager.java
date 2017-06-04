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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
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

	private ByteArrayPool byteArrayPool;
	private MetaInfo allMetaInfo;

	private String storePath;

	private int partitionNum = Config.PARTITION_NUM;

	private DiskFetchService[] fetchServices = new DiskFetchService[partitionNum];
	private Thread[] fetchThreads = new Thread[partitionNum];

	private BlockingQueue<DecompressSuperSegReq>[] decompressReqQueues = new BlockingQueue[partitionNum];
	private DecompressService[] decompressServices = new DecompressService[partitionNum];
	private Thread[] decompressThreads = new Thread[partitionNum];

	/**
	 * bucket queues partition, every msgEncoderService hold a part of bucket
	 * within a specific readBufferQueue.
	 */
	private BlockingQueue<ConsecutiveSegs>[] readConsecutiveSegsQueues;

	private MessageEncoderService[] msgEncoderServices;
	private Thread[] msgEncoderThreads;

	private InputManager() {
		this.storePath = SmartMessageStore.STORE_PATH;
		byteArrayPool = ByteArrayPool.getInstance();

		// init bucketMetas
		allMetaInfo = loadAllMetaInfo(storePath);

		System.out.println(allMetaInfo.queues);
		System.out.println(allMetaInfo.topics);
		processedSegmentNumMap = new HashMap<>(allMetaInfo.queuesSize + allMetaInfo.topicsSize);

		for (String bucket : Config.HACK_BUCKETS) {
			processedSegmentNumMap.put(bucket, new AtomicInteger());
		}

		// start Fetch service and message encoder services

		readConsecutiveSegsQueues = new LinkedBlockingQueue[Config.NUM_ENCODER_MESSAGE_THREAD];
		for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
//			 readConsecutiveSegsQueues[i] = new
//			 LinkedBlockingQueue<>(Config.READ_BUFFER_QUEUE_SIZE);
			readConsecutiveSegsQueues[i] = new LinkedBlockingQueue<>(25);
		}

		msgEncoderServices = new MessageEncoderService[Config.NUM_ENCODER_MESSAGE_THREAD];
		msgEncoderThreads = new Thread[Config.NUM_ENCODER_MESSAGE_THREAD];

		for (int i = 0; i < partitionNum; i++) {

//			 this.decompressReqQueues[i] = new
//			 LinkedBlockingQueue<>(Config.DECOMPRESS_REQUEST_QUEUE_SIZE);
			this.decompressReqQueues[i] = new LinkedBlockingQueue<>(50);

			this.decompressServices[i] = new DecompressService(i);
			this.decompressThreads[i] = new Thread(this.decompressServices[i]);
			this.decompressThreads[i].start();

			this.fetchServices[i] = new DiskFetchService(i);
			this.fetchThreads[i] = new Thread(fetchServices[i]);
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

	class DiskFetchService implements Runnable {

		int rank;

		DiskFetchService(int rank) {
			this.rank = rank;
		}

		@Override
		public void run() {
			HashMap<Integer, FileSuperSeg> fileSuperSegMap = allMetaInfo.getFileSuperSegMap(rank);
			for (int fileId = 0; fileId < allMetaInfo.numDataFiles[rank]; fileId++) {
				FileSuperSeg fileSuperSeg = fileSuperSegMap.get(fileId);
				long start = System.currentTimeMillis();

				String filename = Config.getFileName(storePath, rank, fileId);

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
					decompressReqQueues[rank].put(new DecompressSuperSegReq(rank, fileId, buffer,
							fileSuperSeg.numSegsInSuperSeg, fileSuperSeg.compressedSize));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
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

				int localnumFetchSuperSeg = numFetchSuperSegs.incrementAndGet();
				numFetchSegs.addAndGet(numSegsInSuperSeg);

				long end = System.currentTimeMillis();

				logger.info(String.format("(%dth superseg) Read super-segment data from %s cost %d ms, %d bytes",
						localnumFetchSuperSeg, filename, end - start, fileSuperSeg.compressedSize));
			}

			logger.info("Read all the files, emit finish signal");
			try {
				decompressReqQueues[rank].put(DecompressSuperSegReq.NULL);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	static CountDownLatch decompressLatch = new CountDownLatch(Config.PARTITION_NUM);

	class DecompressService implements Runnable {
		int rank;

		long decompressTotalCost = 0;
		long decompressTotalSize = 0;
		DecompressService(int rank) {
			this.rank = rank;
		}

		@Override
		public void run() {
			HashMap<Integer, FileSuperSeg> fileSuperSegMap = allMetaInfo.getFileSuperSegMap(rank);
			while (true) {
				DecompressSuperSegReq req = null;
				try {
					req = decompressReqQueues[rank].take();
					if (req != DecompressSuperSegReq.NULL) {
						DecompressedSuperSegment dss = new DecompressedSuperSegment(req.buffer, req.numSegsInSuperSeg,
								req.compressedSize);

						long start = System.currentTimeMillis();
						FileSuperSeg fileSuperSeg = fileSuperSegMap.get(req.fileId);

						ByteArrayUnit unit = byteArrayPool.fetchByteArray(fileSuperSeg.numSegsInSuperSeg);
						byte[] superSegmentBinary = unit.data;

						dss.decompress(superSegmentBinary);

						int segCursor = 0;
						for (SequentialSegs sss : fileSuperSeg.sequentialSegs) {
							String bucket = sss.bucket;
							// TODO multi-service
							// distinguish the bucket
							BlockingQueue<ConsecutiveSegs> readConsecutiveSegsQueue = readConsecutiveSegsQueues[Config.BUCKET_RANK_MAP
									.get(bucket)];
							if (readConsecutiveSegsQueue == null) {
								logger.severe("lack corresponding readBufferQueue " + bucket);
							}
							// System.out.printf("rank%d, fileId=%d, bucket=%s,
							// offset=%d, numSegs=%d\n", rank, req.fileId,
							// bucket, segCursor, sss.numSegs);
							readConsecutiveSegsQueue
									.put(new ConsecutiveSegs(rank, req.fileId, bucket, unit, segCursor, sss.numSegs));
							segCursor += sss.numSegs;
						}

						long end = System.currentTimeMillis();
						decompressTotalCost+=(end-start);
						decompressTotalSize +=(fileSuperSeg.compressedSize);
						logger.info(String.format("Decompress (%d->%d), cost %d ms", fileSuperSeg.compressedSize,
								fileSuperSeg.numSegsInSuperSeg * Config.SEGMENT_SIZE, end - start));
					} else {
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					req.buffer = null;
					req = null;
				}
			}
			logger.info(String.format("Rank%d compress cost %d ms, finally size: %d bytes", rank, decompressTotalCost,
					decompressTotalSize));
			decompressLatch.countDown();
			try {
				decompressLatch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (rank == 0) {
				logger.info("All fetch services have been finished");
				for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
					try {
						readConsecutiveSegsQueues[i].put(ConsecutiveSegs.NULL);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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
			long start = System.currentTimeMillis();
			int numMsgs = 0;
			int offsetSegUnit = sc.offsetSegUnit;
			for (int i = 0; i < sc.numSegs; i++) {
				// !!sc.buff may be shared by other threads
				ReadableSegment readSegment = ReadableSegment.wrap(sc.unit.data, (offsetSegUnit + i) * Segment.CAPACITY,
						Segment.CAPACITY);

				if (!sc.bucket.equals(readSegment.bucket)) {
					System.out.printf("E rank%d, fileId=%d, bucket=%s(deserBucket=%s), offset=%d, numSegs=%d,\n",
							sc.rank, sc.fileId, sc.bucket, readSegment.bucket, sc.offsetSegUnit, sc.numSegs);
				} else {
					// System.out.printf("C rank%d, fileId=%d,
					// bucket=%s(deserBucket=%s), offset=%d, numSegs=%d\n",
					// sc.rank, sc.fileId, sc.bucket, readSegment.bucket,
					// sc.offsetSegUnit, sc.numSegs);
				}
				int processedSegmentNum = processedSegmentNumMap.get(sc.bucket).incrementAndGet();
				ArrayList<BlockingQueue<MessagePool>> queueList = bucketBindingMsgQueuesMap.get(sc.bucket);

				MessagePool pool = null;
				Message msg = null;
				try {
					pool = new MessagePool(Config.MAX_MESSAGE_POOL_CAPACITY);
					while (true) {
						msg = readSegment.read();
						numMsgs++;
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
					// System.out.println("bucket " + sc.bucket + " process: " +
					// processedSegmentNum);
					if (processedSegmentNum >= allMetaInfo.bucketMetaMap.get(sc.bucket).numSegs) {
						// System.out.println("bucket ended: " + sc.bucket);
						for (BlockingQueue<MessagePool> queue : queueList)
							try {
								queue.put(MessagePool.nullMessagePool);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					// TODO
					byteArrayPool.returnByteArray(sc.unit);
					readSegment.close();
					readSegment = null;
				}
			}
			long end = System.currentTimeMillis();
			logger.info(String.format("Decoder %d msgs, cost %d ms", numMsgs, end - start));
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
	int rank;
	int fileId;
	int numSegs;
	String bucket;
	ByteArrayUnit unit;
	int offsetSegUnit;

	private ConsecutiveSegs() {

	}

	public ConsecutiveSegs(int rank, int fileId, String bucket, ByteArrayUnit unit, int offsetSegUnit, int numSegs) {
		super();
		this.rank = rank;
		this.fileId = fileId;
		this.numSegs = numSegs;
		this.bucket = bucket;
		this.unit = unit;
		this.offsetSegUnit = offsetSegUnit;
	}

}

class DecompressSuperSegReq {
	int rank;
	int fileId;
	int numSegsInSuperSeg;
	ByteBuffer buffer;
	long compressedSize;

	public static final DecompressSuperSegReq NULL = new DecompressSuperSegReq();

	DecompressSuperSegReq() {

	}

	public DecompressSuperSegReq(int rank, int fileId, MappedByteBuffer buffer, int numSegsInSuperSeg,
			long compressedSize) {
		this.rank = rank;
		this.fileId = fileId;
		this.buffer = buffer;
		this.numSegsInSuperSeg = numSegsInSuperSeg;
		this.compressedSize = compressedSize;
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

class ByteArrayUnit {
	byte[] data;
	int id; // from 0 to Config.DECOMPRESS_BYTE_POOL_SIZE-1

	ByteArrayUnit(int id) {
		super();
		this.data = new byte[Config.REQ_BATCH_COUNT_THRESHOLD * Config.SEGMENT_SIZE];
		this.id = id;
	}

}

class ByteArrayPool {
	private ArrayBlockingQueue<ByteArrayUnit> pool;
	private AtomicInteger[] holdNums;

	private final static ByteArrayPool INSTANCE = new ByteArrayPool();

	private ByteArrayPool() {
		pool = new ArrayBlockingQueue<>(Config.DECOMPRESS_BYTE_POOL_SIZE);
		holdNums = new AtomicInteger[Config.DECOMPRESS_BYTE_POOL_SIZE];
		for (int i = 0; i < Config.DECOMPRESS_BYTE_POOL_SIZE; i++) {
			holdNums[i] = new AtomicInteger();
			try {
				pool.put(new ByteArrayUnit(i));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public ByteArrayUnit fetchByteArray(int holdNum) {
		ByteArrayUnit b = null;
		try {
			b = pool.take();
			holdNums[b.id].set(holdNum);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return b;
	}

	public void returnByteArray(ByteArrayUnit b) {
		try {
			if (holdNums[b.id].decrementAndGet() == 0)
				pool.put(b);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static ByteArrayPool getInstance() {
		return INSTANCE;
	}

}
