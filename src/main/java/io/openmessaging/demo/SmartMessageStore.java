package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.openmessaging.Message;
import io.openmessaging.Producer;

public class SmartMessageStore {
	private static Logger logger = Logger.getGlobal();
	static {
		logger.setLevel(Level.INFO);
	}

	public static final String META_FILE = "seg.meta";

	private static SmartMessageStore INSTANCE;

	static String STORE_PATH;
	static boolean IS_OUTPUT_OR_INPUT;

	// both
	/**
	 * there exist the corresponding msgs in the library. No business with the
	 * register of consumers.
	 */
	Set<String> existQueues, existTopics;

	// ------------------------------------------------------------
	// output
	private int numProducers = 0;
	private int numFlush = 0;
	private OutputManager outputManager;
	private HashMap<String, BucketWriteBox> bucketWriteBoxMap;

	// ------------------------------------------------------------
	// input

	// private Set<String> regQueues, regTopics;

	// private HashMap<String, BucketOccurs> bucketSubMap;
	// private PriorityQueue<BucketOccurs> orderedBucketOccurs; // descending
	// order

	private int numConsumers = 0;
	private InputManager inputManager;
	private HashMap<String, Integer> consumerNullMsgNumMap;

	/**
	 * queue_name (one consumer) -> MessageQueue
	 */
	private HashMap<String, BlockingQueue<Message>> consumerBindingMsgQueueMap;

	/**
	 * bucket (topic or queue) -> MessageQueue list
	 */
	private HashMap<String, ArrayList<BlockingQueue<Message>>> bucketBindingMsgQueuesMap;

	private SmartMessageStore() {

		logger.info("IS_OUTPUT_OR_INPUT : "+ IS_OUTPUT_OR_INPUT);
		if (IS_OUTPUT_OR_INPUT) {
			// output
			this.bucketWriteBoxMap = new HashMap<>(Config.NUM_BUCKETS);

			// TODO hack
			String bucket = null;
			for (int i = 0; i < Config.NUM_QUEUES; i++) {
				bucket = "QUEUE_" + i;
				bucketWriteBoxMap.put(bucket, new BucketWriteBox(bucket));
			}
			for (int i = 0; i < Config.NUM_TOPICS; i++) {
				bucket = "TOPIC_" + i;
				bucketWriteBoxMap.put(bucket, new BucketWriteBox(bucket));
			}
			this.outputManager = OutputManager.getInstance();
			existQueues = new HashSet<>();
			existTopics = new HashSet<>();
		} else {
			// input

			// init queues and topics
			this.inputManager = InputManager.getInstance();
			existQueues = inputManager.getAllMetaInfo().queues;
			existTopics = inputManager.getAllMetaInfo().topics;

			this.consumerNullMsgNumMap = new HashMap<>(Config.NUM_QUEUES);

			this.consumerBindingMsgQueueMap = new HashMap<>(Config.NUM_QUEUES);
			this.bucketBindingMsgQueuesMap = new HashMap<>(Config.NUM_BUCKETS);
			for (String queue : existQueues) {
				consumerNullMsgNumMap.put(queue, 0);
				consumerBindingMsgQueueMap.put(queue, new LinkedBlockingQueue<>(Config.READ_MSG_QUEUE_SIZE));
				bucketBindingMsgQueuesMap.put(queue, new ArrayList<>(Config.NUM_QUEUES));
			}

			for (String topic : existTopics) {
				bucketBindingMsgQueuesMap.put(topic, new ArrayList<>(Config.NUM_QUEUES));
			}
		}
	}

	public static SmartMessageStore getInstance() {
		synchronized (logger) {
			if (INSTANCE == null)
				INSTANCE = new SmartMessageStore();

			if (IS_OUTPUT_OR_INPUT)
				INSTANCE.numProducers++;
			else
				INSTANCE.numConsumers++;
		}
		return INSTANCE;
	}

	public void putMessage(String bucket, Producer p, Message message) {
		bucketWriteBoxMap.get(bucket).cache(p, message);

		// BucketWriteBox box = bucketWriteBoxMap.get(bucket);
		// if (box == null) {
		// BucketWriteBox newWsq = new BucketWriteBox(bucket);
		// box = bucketWriteBoxMap.putIfAbsent(bucket, newWsq);
		// box = (box == null ? newWsq : box);
		// }
		// box.cache(p, message);
	}

	public Message pullMessage(String queue, int bucketSize) {

		// if receive #bucketList NullMessage, then can stop
		Message msg = null;
		try {
			BlockingQueue<Message> bq = consumerBindingMsgQueueMap.get(queue);
			if (bq == null) {
				return null;
			}
			while (true) {
				msg = bq.take();
				if (msg instanceof NullMessage) {
					int numNumMsg = consumerNullMsgNumMap.get(queue) + 1;
					consumerNullMsgNumMap.put(queue, numNumMsg);
					if (numNumMsg == bucketSize) {
						return null;
					} else {
						continue;
					}
				} else {
					return msg;
				}
			}
		} catch (InterruptedException e) {
			return null;
		}
	}

	/**
	 * record all the queues and topics
	 * 
	 * @param queues
	 *            queues from a producer
	 * @param topics
	 *            topics from a producer
	 */
	public synchronized void record(Set<String> queues, Set<String> topics) {
		this.existQueues.addAll(queues);
		this.existTopics.addAll(topics);
	}

	/**
	 * {@link io.openmessaging.demo.SmartMessageStore#record(Set, Set)} executes
	 * before flush
	 * 
	 */
	public synchronized void flush() {
		numFlush++;
		if (numFlush < numProducers) {
			logger.info(
					String.format("Try to flush, but not. numFlush = %d, numProducers = %d", numFlush, numProducers));
			return;
		} else {
			// only the last flush producer will trigger the actual flush
			logger.info("Start actual flush");
			long start = System.currentTimeMillis();

			// flush the cache
			for (BucketWriteBox bucketBox : bucketWriteBoxMap.values()) {
				bucketBox.flush();
			}
			outputManager.flush(existQueues, existTopics);
			long end = System.currentTimeMillis();
			logger.info(String.format("Actual flush cost %d ms", end - start));
		}
	}

	int numRegister = 0;

	// TODO, keep sync with other services and don't waste time
	/**
	 * Get the hottest bucket and init the bucket read queue.
	 * 
	 * 1. wait all the numQueues of consumers' registers. We can use the
	 * numBuckets queues to serve the consumers, because the consumers are able
	 * to keep the same pace to consume the message in one topic. 2. execute
	 * along with coming registers. We should use numConsumers queues to
	 * distinguish the message even from the same topic, because of the
	 * different paces.
	 *
	 * Now we choose the strategy 1, assuming that the consumers registers
	 * almost in the same time.
	 */
	public synchronized void register(String queueName, List<String> bucketList) {
		// every consumer's queue differs
		numRegister++;

		// the binding queue must be in the library
		if (existQueues.contains(queueName)) {
			for (String bucket : bucketList) {
				ArrayList<BlockingQueue<Message>> bq = bucketBindingMsgQueuesMap.get(bucket);
				if (bq == null) {
					// there dosen't exist this bucket
					// ignore
				} else {
					bq.add(consumerBindingMsgQueueMap.get(queueName));
				}
			}
		}

		if (numRegister < existQueues.size()) {
			logger.info(queueName + " has been registered, wait others");
			return;
		} else {
			logger.info(queueName + " has been registered, can start pull services");
			inputManager.startPullService(bucketBindingMsgQueuesMap);
		}
	}

	class BucketOccurs implements Comparable<BucketOccurs> {
		String bucket;
		int occurs = 0;

		public BucketOccurs(String bucket) {
			super();
			this.bucket = bucket;
		}

		public void incrCnt() {
			occurs++;
		}

		@Override
		public int compareTo(BucketOccurs o) {
			// for descending order
			return o.occurs - occurs;
		}
	}
}
