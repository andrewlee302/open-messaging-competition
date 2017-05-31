package io.openmessaging.demo;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import io.openmessaging.Message;

// Segment queue for a specific bucket
public class WriteSegmentQueue extends LinkedBlockingQueue<WritableSegment> {

	private static final long serialVersionUID = 8386318494174264432L;
	private static Logger logger = Logger.getGlobal();

	private String bucket;
	private OutputManager outputManager = OutputManager.getInstance();

	// All segments' size: 10 * 1M * 100 = 1G
	// 40000000 message needs 4000 segment if size of a
	// segment a message is 1 MByte and 100 bytes,
	// so every bucket needs 40 segments averagely.
	private final static int DEFAULT_INIT_SIZE = 10;

	private WritableSegment currSegment;
	private int initSize;
	private int msgIndex = 0; // start from 0

	public WriteSegmentQueue(String bucket) {
		this(bucket, DEFAULT_INIT_SIZE);
		outputManager = OutputManager.getInstance();
	}

	public WriteSegmentQueue(String bucket, int initSize) {
		super();
		this.bucket = bucket;
		this.initSize = initSize;
		for (int i = 0; i < initSize; i++) {
			try {
				this.put(new WritableSegment(bucket));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			currSegment = this.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Cache the message into an available segment, if the segment size reaches
	 * a threshold, then it will be send to i/o manager.
	 * 
	 * @param msg
	 * @throws InterruptedException
	 */
	public synchronized void cache(Message msg) {
		try {
			currSegment.append(msg);
			msgIndex++;
		} catch (SegmentFullException e) {
			// send segment to i/o manager
			outputManager.writeSegment(this, bucket, currSegment, msgIndex);
			try {
				this.currSegment = this.take();
				this.currSegment.append(msg);
			} catch (SegmentFullException e2) {
				// TODO
				logger.severe(String.format("Message(%d byte) is too big to be stored in a segment(%d byte)", 1, 2));
				System.exit(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * flush the current segment of the specific bucket to output manager
	 */
	public void flush() {
		// send segment to i/o manager
		outputManager.writeSegment(this, bucket, currSegment, msgIndex);
		currSegment = null;
	}
}
