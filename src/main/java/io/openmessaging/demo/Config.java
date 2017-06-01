package io.openmessaging.demo;

public class Config {

	// all below are estimated values, not the fact
	public static final int NUM_PRODUCERS = 20;
	public static final int NUM_CONSUMERS = 10;

	public static final int NUM_QUEUES = NUM_CONSUMERS;
	public static final int NUM_TOPICS = 90;
	public static final int NUM_BUCKETS = NUM_QUEUES + NUM_TOPICS;

	public static final int MAXIMUM_SIZE_BUCKET_NAME = 20; // 10 char

	// tuning!
	public static final int WRITE_REQUEST_QUEUE_SIZE = 32;
	public static final int REQ_BATCH_COUNT_THRESHOLD = 64;
	public static final long REQ_WAIT_TIME_THRESHOLD = 300; // 500ms
	public static final int WRITE_SEGMENT_QUEUE_SIZE = 20;
	/**
	 *  it directly relates to the messages' lifecycle.
	 */
	public static final int SEGMENT_SIZE = 1 << 18;

	public static final int AVERAGE_MSG_SIZE = 157;


	public static final int NUM_ENCODER_MESSAGE_THREAD = 8;
	public static final int READ_BUFFER_QUEUE_SIZE = 1000; // very large
	public static final int READ_MSG_QUEUE_SIZE = Integer.MAX_VALUE;

	// useless temporarily
	public static final int SEGMENT_READ_QUEUE_SIZE = 10;

}
