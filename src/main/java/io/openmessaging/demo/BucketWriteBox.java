package io.openmessaging.demo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.openmessaging.Message;
import io.openmessaging.Producer;

/**
 * Segment queue for a specific bucket, one free queue, and one wait queue to be
 * persisted
 * 
 * @author andrew
 *
 */
public class BucketWriteBox {

	private static Logger logger = Logger.getGlobal();

	static AtomicInteger occurMetaNotEnough = new AtomicInteger();
	static AtomicInteger occurContentNotEnough = new AtomicInteger();

	private OutputManager outputManager = OutputManager.getInstance();
	private String bucket;
	private LinkedBlockingQueue<WritableSegment> freeQueue;

	private ConcurrentHashMap<Producer, WritableSegment> currentWriteSegsMap;

	private int msgIndex = 0; // start from 0

	public BucketWriteBox(String bucket) {
		this(bucket, Config.WRITE_SEGMENT_QUEUE_SIZE);
	}

	public BucketWriteBox(String bucket, int initSize) {
		this.outputManager = OutputManager.getInstance();
		this.bucket = bucket;
		this.currentWriteSegsMap = new ConcurrentHashMap<>(Config.NUM_PRODUCERS);

		this.freeQueue = new LinkedBlockingQueue<>(initSize);

		for (int i = 0; i < initSize; i++) {
			try {
				this.freeQueue.put(new WritableSegment(bucket));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Cache the message into an available segment, if the segment size reaches
	 * a threshold, then it will be send to i/o manager.
	 * 
	 * @param producer
	 *            current producer binding a unique thread
	 * @param msg
	 * @throws InterruptedException
	 */
	public void cache(Producer p, Message msg) {
		WritableSegment currSegment = currentWriteSegsMap.get(p);
		if (currSegment == null) {
			try {
				currSegment = freeQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			currentWriteSegsMap.put(p, currSegment);
		}

		// !!! optimization, keep the order in the a producer
		// reduce the contention
		try {
			currSegment.append(msg);
			msgIndex++;
		} catch (SegmentFullException e) {
			if (e.metaNotEnought)
				occurMetaNotEnough.incrementAndGet();
			else
				occurContentNotEnough.incrementAndGet();

			// send segment to i/o manager
			outputManager.writeSegment(this.freeQueue, bucket, currSegment, msgIndex);
			try {
				currSegment = freeQueue.take();
				currentWriteSegsMap.put(p, currSegment);
				currSegment.append(msg);
				msgIndex++;
			} catch (SegmentFullException e2) {
				logger.severe("Message is too big to be stored in a segment");
				System.exit(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * flush the current segment of the specific bucket from all producers to
	 * output manager
	 */
	public void flush() {
		// send segment to i/o manager
		for (WritableSegment currSegment : currentWriteSegsMap.values()) {
			outputManager.writeSegment(this.freeQueue, bucket, currSegment, msgIndex);
			currSegment = null;
		}
	}
}
