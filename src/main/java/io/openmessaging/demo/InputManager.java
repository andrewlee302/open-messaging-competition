package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
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

	private MetaInfo allMetaInfo;

	private String storePath;

	private DiskFetchService[] fetchServices;
	private Thread[] fetchThreads;
	private HashMap<String, Integer> bucketRankMap;

	/**
	 * bucket queues partition, every msgEncoderService hold a part of bucket
	 * within a specific readBufferQueue.
	 */
	private BlockingQueue<MappedByteBufferStruct>[] readBufferQueues;
	private HashMap<String, BlockingQueue<MappedByteBufferStruct>> bucketReadBufferQueueMap;
	private MessageEncoderService[] msgEncoderServices;
	private Thread[] msgEncoderThreads;

	private InputManager() {
		this.storePath = SmartMessageStore.STORE_PATH;

		// init bucketMetas
		loadAllMetaInfo();

		System.out.println(allMetaInfo.queues);
		System.out.println(allMetaInfo.topics);
		processedSegmentNumMap = new HashMap<>(allMetaInfo.queuesSize + allMetaInfo.topicsSize);

		for (String bucket : Config.HACK_BUCKETS) {
			processedSegmentNumMap.put(bucket, new AtomicInteger());
		}

		// start Fetch service and message encoder services
		this.fetchServices = new DiskFetchService[Config.NUM_READ_DISK_THREAD];
		this.fetchThreads = new Thread[Config.NUM_READ_DISK_THREAD];
		for (int i = 0; i < Config.NUM_READ_DISK_THREAD; i++) {
			this.fetchServices[i] = new DiskFetchService(i);
			this.fetchThreads[i] = new Thread(fetchServices[i]);
		}

		readBufferQueues = new BlockingQueue[Config.NUM_ENCODER_MESSAGE_THREAD];
		for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
			readBufferQueues[i] = new LinkedBlockingQueue<>(Config.READ_BUFFER_QUEUE_SIZE);
		}

		// uniform distribution
		bucketRankMap = new HashMap<>(Config.NUM_BUCKETS);
		bucketReadBufferQueueMap = new HashMap<>(Config.NUM_BUCKETS);
		int tempCnt = 0;
		for (String bucket : Config.HACK_BUCKETS) {
			bucketReadBufferQueueMap.put(bucket, readBufferQueues[tempCnt++ % Config.NUM_ENCODER_MESSAGE_THREAD]);
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
			msgEncoderServices[i] = new MessageEncoderService(readBufferQueues[i], bucketBindingMsgQueuesMap);
			msgEncoderThreads[i] = new Thread(msgEncoderServices[i]);
			msgEncoderThreads[i].start();
		}
	}

	private void loadAllMetaInfo() {
		logger.info("Start load the meta info");

		long start = System.currentTimeMillis();
		RandomAccessFile memoryMappedFile = null;
		MappedByteBuffer buffer = null;
		Path p = Paths.get(storePath, SmartMessageStore.META_FILE);
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
			ois = new ObjectInputStream(new MappedByteBufferInputStream(buffer));
			allMetaInfo = (MetaInfo) ois.readObject();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		try {
			ois.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
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
			for (int fileId = 0; fileId < allMetaInfo.numDataFiles; fileId++) {
				FileSuperSeg fileSuperSeg = fileSuperSegMap.get(fileId);
				long start = System.currentTimeMillis();
				Path p = Paths.get(storePath, fileId + ".data");
				String filename = p.toString();

				int numSegsInSuperSeg = fileSuperSeg.numSegsInSuperSeg;

				long readFileSize = numSegsInSuperSeg * Segment.CAPACITY;
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

				int segCursor = 0;
				for (SequentialSegs sss : fileSuperSeg.sequentialSegs) {
					String bucket = sss.bucket;
					if (bucketRankMap.get(bucket) != rank) {
						segCursor += sss.numSegs;
						continue;
					}
					// only the same thread process the same segments
					RandomAccessFile memoryMappedFile = null;
					try {
						memoryMappedFile = new RandomAccessFile(filename, "r");
						MappedByteBuffer buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
								segCursor * Segment.CAPACITY, sss.numSegs * Segment.CAPACITY);
						// load the segments into the physical memory
						// TODO, when to put the buffer, avoiding the page swap.
						buffer.load();

						// logger.info(
						// String.format("file = %d, bucket = %s, numSegs = %d",
						// fileId, sss.bucket, sss.numSegs));
						BlockingQueue<MappedByteBufferStruct> readBufferQueue = bucketReadBufferQueueMap.get(bucket);
						if (readBufferQueue == null) {
							logger.severe("lack corresponding readBufferQueue " + bucket);
						}
						readBufferQueue.put(new MappedByteBufferStruct(bucket, buffer, 0, sss.numSegs));
						segCursor += sss.numSegs;

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							memoryMappedFile.close();
						} catch (IOException e) {
							memoryMappedFile = null;
						}
					}
				}

				numFetchSegs.addAndGet(numSegsInSuperSeg);
				numFetchSuperSegs.incrementAndGet();

				long end = System.currentTimeMillis();

				logger.info(String.format("(%dth superseg, %dth seg) Read super-segment data from %s cost %d ms",
						numFetchSuperSegs.get(), numFetchSegs.get(), filename, end - start, readFileSize));
			}
			diskFetchlatch.countDown();
			try {
				diskFetchlatch.await();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

			if (rank == 0) {
				logger.info("Read all the files, emit finish signal");
				for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
					try {
						readBufferQueues[i].put(new NullMappedByteBufferStruct());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	class MessageEncoderService implements Runnable {
		HashMap<String, ArrayList<BlockingQueue<MessagePool>>> bucketBindingMsgQueuesMap;
		BlockingQueue<MappedByteBufferStruct> readBufferQueue;

		public MessageEncoderService(BlockingQueue<MappedByteBufferStruct> readBufferQueue,
				HashMap<String, ArrayList<BlockingQueue<MessagePool>>> bucketBindingMsgQueuesMap) {
			this.readBufferQueue = readBufferQueue;
			this.bucketBindingMsgQueuesMap = bucketBindingMsgQueuesMap;
		}

		@Override
		public void run() {
			while (true) {
				MappedByteBufferStruct bufferStruct = null;
				try {
					bufferStruct = readBufferQueue.take();
					if (bufferStruct instanceof NullMappedByteBufferStruct) {
						break;
					} else {
						processSequentialSegments(bufferStruct.bucket, bufferStruct.buffer,
								bufferStruct.offsetInMappedByffer, bufferStruct.segNum);
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
		private void processSequentialSegments(String bucket, MappedByteBuffer buffer, int offsetInMappedByffer,
				int segNum) {
			for (int i = 0; i < segNum; i++) {
				ReadableSegment readSegment = ReadableSegment.wrap(buffer, offsetInMappedByffer + i * Segment.CAPACITY,
						Segment.CAPACITY);
				String b = readSegment.bucket;
				if (!b.equals(bucket)) {
					logger.warning("Error " + b + " != " + bucket);
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

	class MappedByteBufferStruct {
		/**
		 * the new buffer, not derived from basic buffer
		 */
		MappedByteBuffer buffer;
		/**
		 * the offset is not the file offset ( as the super-segment) is the
		 * offset of the new bytebuffer
		 */
		int offsetInMappedByffer;
		int segNum;
		String bucket;

		MappedByteBufferStruct() {

		}

		MappedByteBufferStruct(String bucket, MappedByteBuffer buffer, int offset, int segNum) {
			this.bucket = bucket;
			this.buffer = buffer;
			this.offsetInMappedByffer = offset;
			this.segNum = segNum;
		}

		MappedByteBufferStruct(MappedByteBuffer buffer, int offset, int length) {
			this.buffer = buffer;
			this.offsetInMappedByffer = offset;
			this.segNum = segNum;
		}
	}

	class NullMappedByteBufferStruct extends MappedByteBufferStruct {

		public NullMappedByteBufferStruct() {
			super();
		}
	}

	class MappedByteBufferAndNumSegs {
		MappedByteBuffer buffer;
		int numSegs;

		MappedByteBufferAndNumSegs() {

		}

		public MappedByteBufferAndNumSegs(MappedByteBuffer buffer, int numSegs) {
			this.buffer = buffer;
			this.numSegs = numSegs;
		}
	}

	class NullMappedByteBufferAndNumSegs extends MappedByteBufferAndNumSegs {

		public NullMappedByteBufferAndNumSegs() {
		}
	}

	// class MessageEncoderService implements Runnable {
	//
	// BucketOccurs bucketOccurs;
	// BucketMeta meta;
	// int numReadSegs;
	// ReadSegmentQueue readSemengQueue;
	//
	// /**
	// *
	// * @param bucket
	// * the specific bucket (queue or topic)
	// * @param numReadSegs
	// * will read the number of segments of the bucket. If is
	// * zero, then read all segments until the end.
	// */
	// MessageEncoderService(BucketOccurs bucketOccurs, int numReadSegs) {
	// this.bucketOccurs = bucketOccurs;
	// this.numReadSegs = numReadSegs;
	// this.meta = bucketMetas.get(bucketOccurs.bucket);
	// readSemengQueue = readSegmentQueueMap.get(bucketOccurs.bucket);
	// if (readSemengQueue == null) {
	// readSemengQueue = new ReadSegmentQueue();
	// readSegmentQueueMap.put(bucketOccurs.bucket, readSemengQueue);
	// }
	// }
	//
	// @Override
	// public void run() {
	// List<Integer> metaContent = meta.content;
	// for (int i = 0; i < metaContent.size(); i +=
	// BucketMeta.META_INT_UNIT_SIZE) {
	// int index = metaContent.get(i);
	// int fileId = metaContent.get(i + 1);
	// int offset = metaContent.get(i + 2);
	// int numSegs = metaContent.get(i + 3);
	// readReqQueue.put(new ReadRequest(index, fileId, offset, numSegs));
	// }
	//
	// }
	//
	// // TODO load async
	// // buffer.load();
	//
	// // directly encapsulate the mapped memory to Segment
	// byte[] data = buffer.array();for(
	// int i = 0;i<numSegs;i++)
	// {
	// ReadableSegment readSegment = ReadableSegment.wrap(data, i *
	// Segment.CAPACITY, Segment.CAPACITY);
	// try {
	// readSemengQueue.put(readSegment);
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// }try
	// {
	// memoryMappedFile.close();
	// }catch(
	// IOException e)
	// {
	// e.printStackTrace();
	// }
	// long end = System
	// .currentTimeMillis();logger.info(String.format("Write to disk cost %d ms,
	// size %d bytes",end-start,fileSize));
	// }
	// }
	//
	// }

	class ReadRequest implements Comparable<ReadRequest> {
		// the same with one unit of the BucketMeta
		int index;
		int fileId;
		int offset;
		int numSegs;

		public ReadRequest(int index, int fileId, int offset, int numSegs) {
			super();
			this.index = index;
			this.fileId = fileId;
			this.offset = offset;
			this.numSegs = numSegs;
		}

		@Override
		public int compareTo(ReadRequest o) {
			// TODO Auto-generated method stub
			return 0;
		}
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
