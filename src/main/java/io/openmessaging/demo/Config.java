package io.openmessaging.demo;

import java.util.HashSet;
import java.util.Set;

public class Config {

	// all below are estimated values, not the fact
	public static final int NUM_PRODUCERS = 10;
	public static final int NUM_CONSUMERS = 10;

	public static final int NUM_QUEUES = NUM_CONSUMERS;
	public static final int NUM_TOPICS = 90;
	public static final int NUM_BUCKETS = NUM_QUEUES + NUM_TOPICS;

	public static final int MAXIMUM_SIZE_BUCKET_NAME = 20; // 10 char

	// tuning!
	public static final int WRITE_REQUEST_QUEUE_SIZE = 64;
	public static final int REQ_BATCH_COUNT_THRESHOLD = 128;
	public static final long REQ_WAIT_TIME_THRESHOLD = 300; // ms
	public static final int WRITE_SEGMENT_QUEUE_SIZE = 10 * 10;

	/**
	 *  it directly relates to the messages' lifecycle.
	 */
	public static final int SEGMENT_SIZE = 1 << 16;

	public static final int AVERAGE_MSG_SIZE = 100;

	public static final int DEFAULT_KEYVALUE_MAP_SIZE = 18;

	public static final int NUM_ENCODER_MESSAGE_THREAD = 16;
	public static final int READ_BUFFER_QUEUE_SIZE = 10000; // very large
	public static final int READ_MSG_QUEUE_SIZE = Integer.MAX_VALUE;

	// useless temporarily
	public static final int SEGMENT_READ_QUEUE_SIZE = 10;
	
	// All segments' size: 10 * 1M * 100 = 1G
	// 40000000 message needs 4000 segment if size of a
	// segment a message is 1 MByte and 100 bytes,
	// so every bucket needs 40 segments averagely.
	// WRITE_SEGMENT_QUEUE_SIZE
	
	
	// -----------------------------------------
	public final static Set<String> HACK_BUCKETS = new HashSet<>(Config.NUM_BUCKETS);
	public final static Set<String> HACK_QUEUES = new HashSet<>(Config.NUM_QUEUES);
	public final static Set<String> HACK_TOPICS = new HashSet<>(Config.NUM_TOPICS);
	static {
		for (int i = 0; i < Config.NUM_QUEUES; i++) {
			String bucket = "QUEUE_" + i;
			HACK_BUCKETS.add(bucket);
			HACK_QUEUES.add(bucket);
		}
		for (int i = 0; i < Config.NUM_TOPICS; i++) {
			String bucket = "TOPIC_" + i;
			HACK_BUCKETS.add(bucket);
			HACK_TOPICS.add(bucket);
		}
	}
}
