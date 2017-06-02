package io.openmessaging.demo;

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

	private DiskFetchService fetchService;
	private Thread fetchThread;

	/**
	 * bucket queues partition, every msgEncoderService hold a part of bucket
	 * within a specific readBufferQueue.
	 */
	private BlockingQueue<MappedByteBufferStruct>[] readBufferQueues;
	private HashMap<Byte, BlockingQueue<MappedByteBufferStruct>> bucketReadBufferQueueMap;
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
		this.fetchService = new DiskFetchService();
		this.fetchThread = new Thread(fetchService);

		readBufferQueues = new BlockingQueue[Config.NUM_ENCODER_MESSAGE_THREAD];
		for (int i = 0; i < Config.NUM_ENCODER_MESSAGE_THREAD; i++) {
			readBufferQueues[i] = new LinkedBlockingQueue<>(Config.READ_BUFFER_QUEUE_SIZE);
		}

		// uniform distribution
		bucketReadBufferQueueMap = new HashMap<>(Config.NUM_BUCKETS);
		int tempCnt = 0;
		for (String bucket : Config.HACK_BUCKETS) {
			bucketReadBufferQueueMap.put((byte) bucket.hashCode(),
					readBufferQueues[tempCnt++ % Config.NUM_ENCODER_MESSAGE_THREAD]);
		}

		msgEncoderServices = new MessageEncoderService[Config.NUM_ENCODER_MESSAGE_THREAD];
		msgEncoderThreads = new Thread[Config.NUM_ENCODER_MESSAGE_THREAD];

		this.fetchThread.start();

	}

	public static InputManager getInstance() {
		if (INSTANCE == null)
			INSTANCE = new InputManager();
		return INSTANCE;
	}

	public void startPullService(HashMap<String, ArrayList<BlockingQueue<Message>>> bucketBindingMsgQueuesMap) {
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

	class DiskFetchService implements Runnable {

		long totalReadDiskCost = 0; // ms
		long totalReadDiskSize = 0; // bytes

		@Override
		public void run() {
			for (int fileId = 0; fileId < allMetaInfo.numDataFiles; fileId++) {
				long start = System.currentTimeMillis();
				RandomAccessFile memoryMappedFile = null;
				Path p = Paths.get(storePath, fileId + ".data");
				String filename = p.toString();
				long fileSize = 0;
				try {
					memoryMappedFile = new RandomAccessFile(filename, "r");
					fileSize = memoryMappedFile.length();
					// load the segments into the physical memory
					// TODO, when to put the buffer, avoiding the page swap.
					int numSegs = (int) (fileSize / Segment.CAPACITY);
					for (int i = 0; i < numSegs; i++) {
						MappedByteBuffer buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
								i * Segment.CAPACITY, Segment.CAPACITY);
						buffer.load();
						bucketReadBufferQueueMap.get(allMetaInfo.getNextBucketPart())
								.put(new MappedByteBufferStruct(buffer, 0, Segment.CAPACITY));
					}
					numFetchSegs.addAndGet(numSegs);
					numFetchSuperSegs.incrementAndGet();
					memoryMappedFile.close();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					memoryMappedFile = null;
				}

				long end = System.currentTimeMillis();

				totalReadDiskCost += (end - start);
				totalReadDiskSize += fileSize;

				logger.info(String.format(
						"(%dth superseg, %dth seg) Read super-segment data from %s cost %d ms, size %d bytes, readRate: %.3f m/s",
						numFetchSuperSegs.get(), numFetchSegs.get(), filename, end - start, fileSize,
						((double) totalReadDiskSize) / (1 << 20) / totalReadDiskCost * 1000));
			}
			Byte mustNull = allMetaInfo.getNextBucketPart();
			if (mustNull != null) {
				logger.info("Must be wrong");
			}

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

	class MessageEncoderService implements Runnable {
		HashMap<String, ArrayList<BlockingQueue<Message>>> bucketBindingMsgQueuesMap;
		BlockingQueue<MappedByteBufferStruct> readBufferQueue;

		public MessageEncoderService(BlockingQueue<MappedByteBufferStruct> readBufferQueue,
				HashMap<String, ArrayList<BlockingQueue<Message>>> bucketBindingMsgQueuesMap) {
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
						processSegment(bufferStruct.buffer, bufferStruct.offsetInSuperSegment, bufferStruct.segLength);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("Encoder service " + Thread.currentThread().toString() + " ended.");
		}

		/**
		 * process a single segments
		 * 
		 * @param buffer
		 * @param offsetInSuperSegment
		 * @param segLength
		 */
		private void processSegment(MappedByteBuffer buffer, int offsetInSuperSegment, int segLength) {
			ReadableSegment readSegment = ReadableSegment.wrap(buffer, offsetInSuperSegment, segLength);
			String b = readSegment.bucket;
			int processedSegmentNum = processedSegmentNumMap.get(b).incrementAndGet();
			ArrayList<BlockingQueue<Message>> queueList = bucketBindingMsgQueuesMap.get(b);
			Message msg;
			try {
				while (true) {
					msg = readSegment.read();
					for (BlockingQueue<Message> queue : queueList)
						queue.put(msg);
				}
			} catch (SegmentEmptyException e) {
				// send the nullMessage as a signal
				if (processedSegmentNum >= allMetaInfo.get(b).numSegs) {
					for (BlockingQueue<Message> queue : queueList)
						try {
							queue.put(new NullMessage(null));
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * process all segments in a super-segment
		 * 
		 * @param bufferAndNumSegs
		 *            buffer
		 */
		@SuppressWarnings("unused")
		private void processSuperSegment(MappedByteBufferAndNumSegs bufferAndNumSegs) {
			long start = System.currentTimeMillis();
			MappedByteBuffer buffer = bufferAndNumSegs.buffer;
			int numSegs = bufferAndNumSegs.numSegs;

			int localNumSuperSegs = numConsumeSuperSegs.incrementAndGet();
			int localNumSegs = numConsumeSegs.addAndGet(numSegs);

			int superSegmentSize = numSegs * Segment.CAPACITY;
			for (int i = 0; i < numSegs; i++) {
				processSegment(buffer, i * Segment.CAPACITY, Segment.CAPACITY);
			}
			long end = System.currentTimeMillis();
			logger.info(String.format("(%dth superseg, %dth seg) decode super-segment cost %d ms, size %d bytes",
					localNumSuperSegs, localNumSegs, end - start, superSegmentSize));
		}
	}

	class MappedByteBufferStruct {
		MappedByteBuffer buffer;
		int offsetInSuperSegment;
		int segLength;

		MappedByteBufferStruct() {

		}

		MappedByteBufferStruct(MappedByteBuffer buffer, int offset, int length) {
			this.buffer = buffer;
			this.offsetInSuperSegment = offset;
			this.segLength = length;
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
