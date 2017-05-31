package io.openmessaging.demo;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import io.openmessaging.Message;

public class ReadSegmentQueue extends LinkedBlockingQueue<ReadableSegment> {
	private static final long serialVersionUID = -5853156288776310191L;

	private static Logger logger = Logger.getGlobal();

	private ReadableSegment currSegment;

	public ReadSegmentQueue() {
		super(Config.SEGMENT_READ_QUEUE_SIZE);
	}

	public Message takeMsg() {
		Message msg = null;
		try {
			msg = currSegment.read();
		} catch (SegmentEmptyException e) {
			e.printStackTrace();
			try {
				currSegment = take();
				msg = currSegment.read();
			} catch (SegmentEmptyException e1) {
				logger.severe("No message in the segment");
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		return msg;
	}

}
