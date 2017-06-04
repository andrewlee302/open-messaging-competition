package io.openmessaging.demo;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Config {

	// all below are estimated values, not the fact
	public static final int NUM_PRODUCERS = 10;
	public static final int NUM_CONSUMERS = 10;

	public static final int NUM_QUEUES = NUM_CONSUMERS;
	public static final int NUM_TOPICS = 90;
	public static final int NUM_BUCKETS = NUM_QUEUES + NUM_TOPICS;

	public static final int AVERAGE_MSG_SIZE = 100;
	public static final int MAXIMUM_SIZE_BUCKET_NAME = 20; // 10 char

	// tuning!
	public static final int PARTITION_NUM = 10;

	// write
	public static final int PERSIST_REQUEST_QUEUE_SIZE = Integer.MAX_VALUE;
	public static final int COMPRESS_REQUEST_QUEUE_SIZE = Integer.MAX_VALUE;

	// read
	// 1. !!! will affect page cache in OS
	// totally 512M
	public static final int DECOMPRESS_BYTE_POOL_SIZE = 128; 
	// 2
	public static final int READ_BUFFER_QUEUE_SIZE = 100;
	// 3. 500 super segments
	public static final int DECOMPRESS_REQUEST_QUEUE_SIZE = 250;

	public static final int REQ_BATCH_COUNT_THRESHOLD = 64;
	public static final long REQ_WAIT_TIME_THRESHOLD = 100; // ms

	public static final int WRITE_SEGMENT_QUEUE_SIZE = 10 * 20;


	/**
	 * it directly relates to the messages' lifecycle.
	 */
	public static final int SEGMENT_SIZE = 1 << 14;


	public static final int NUM_ENCODER_MESSAGE_THREAD = PARTITION_NUM;
	public static final int NUM_READ_DISK_THREAD = 16;

	/**
	 * 10(#Consumer) * READ_MSG_QUEUE_SIZE * MAX_MESSAGE_POOL_CAPACITY
	 * * Message_SIZE(200)
	 */
	public static final int MAX_MESSAGE_POOL_CAPACITY = 64;
	public static final int READ_MSG_QUEUE_SIZE = 200;
	// All segments' size: 10 * 1M * 100 = 1G
	// 40000000 message needs 4000 segment if size of a
	// segment a message is 1 MByte and 100 bytes,
	// so every bucket needs 40 segments averagely.
	// WRITE_SEGMENT_QUEUE_SIZE

	// -----------------------------------------
	public final static HashMap<String, Integer> BUCKET_RANK_MAP = new HashMap<>(Config.NUM_BUCKETS);
	public final static Set<String> HACK_BUCKETS = new HashSet<>(Config.NUM_BUCKETS);
	public final static Set<String> HACK_QUEUES = new HashSet<>(Config.NUM_QUEUES);
	public final static Set<String> HACK_TOPICS = new HashSet<>(Config.NUM_TOPICS);
	static {
		int cnt = 0;
		for (int i = 0; i < Config.NUM_QUEUES; i++) {
			String bucket = "QUEUE_" + i;
			HACK_BUCKETS.add(bucket);
			HACK_QUEUES.add(bucket);
			BUCKET_RANK_MAP.put(bucket, cnt % PARTITION_NUM);
			cnt++;
		}
		for (int i = 0; i < Config.NUM_TOPICS; i++) {
			String bucket = "TOPIC_" + i;
			HACK_BUCKETS.add(bucket);
			HACK_TOPICS.add(bucket);
			BUCKET_RANK_MAP.put(bucket, cnt % PARTITION_NUM);
			cnt++;
		}
	}

	public static String getFileName(String dir, int rank, int fileId) {
		return Paths.get(dir, "" + rank + "_" + fileId + ".data").toString();
	}
}
