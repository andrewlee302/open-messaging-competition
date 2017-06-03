package io.openmessaging.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;

import io.openmessaging.Message;

/**
 * | nameLength | bucketName | num | offsets | msg | msg | ... | The length of
 * the bucket name can't exceed MAXIMUM_SIZE_BUCKET_NAME
 * 
 * @author andrew
 *
 */
public abstract class Segment {
	protected static Logger logger = Logger.getGlobal();

	// all offset is the relative offset in the segment
	static final int CAPACITY = Config.SEGMENT_SIZE;
	static final int QUANTITY_SIZE = 4;
	static final int AVERAGE_MSG_SIZE = Config.AVERAGE_MSG_SIZE;
	static final int MAX_NUM_MSG = (CAPACITY - QUANTITY_SIZE * 2 - Config.MAXIMUM_SIZE_BUCKET_NAME)
			/ (AVERAGE_MSG_SIZE + 4);
	static final int OFFSET_REGION_SIZE = MAX_NUM_MSG * 4;
	static final int MSG_REGION_SIZE = CAPACITY - QUANTITY_SIZE * 2 - Config.MAXIMUM_SIZE_BUCKET_NAME
			- OFFSET_REGION_SIZE;
	static final int MSG_REGION_OFFSET = Config.MAXIMUM_SIZE_BUCKET_NAME + QUANTITY_SIZE * 2 + OFFSET_REGION_SIZE;

	String bucket = null;
	int numMsgs = 0; // actual quantity

	/**
	 * msg byte offer in this segment
	 */
	int[] offsets;

	Segment() {
		offsets = new int[MAX_NUM_MSG];
	}
}

class WritableSegment extends Segment {
	byte[] buff;
	ByteBuffer msgBuffer;
	int msgWriteCursor = MSG_REGION_OFFSET;

	WritableSegment(String bucket) {
		super();
		this.bucket = bucket;
		buff = new byte[CAPACITY];
		msgBuffer = ByteBuffer.wrap(buff);

	}

	public void append(Message message) throws SegmentFullException {
		if (numMsgs >= MAX_NUM_MSG) {
			throw new SegmentFullException(true);
		}
		DefaultBytesMessage msg = (DefaultBytesMessage) message;
		int len = Segment.CAPACITY - msgWriteCursor;
		int msgSize = msg.serializeToArray(buff, msgWriteCursor, len);
		if (msgSize != 0) {
			// logger.info(String.format("succ msgWriteCursor = %d, len = %d",
			// msgWriteCursor, len));
			// byte[] temp = new byte[msgSize];
			// System.arraycopy(buff, msgWriteCursor, temp, 0, msgSize);
			// DefaultBytesMessage newMsg =
			// DefaultBytesMessage.deserializeToMsg(temp);
			// logger.info(new String(temp));
			offsets[numMsgs++] = msgWriteCursor;
			msgWriteCursor += msgSize;
		} else {
			// logger.info(String.format("fail msgWriteCursor = %d, len = %d",
			// msgWriteCursor, len));
			throw new SegmentFullException(false);
		}

	}

	// for write request
	public byte[] assemble() {
		// TODO optimize, compact
		msgBuffer.position(0);
		IntBuffer ib = msgBuffer.asIntBuffer();
		int bucketNameLen = bucket.length();
		ib.put(bucketNameLen);

		if (bucketNameLen * 2 > Config.MAXIMUM_SIZE_BUCKET_NAME) {
			logger.severe(bucket + " Exceeds the Config.MAXIMUM_SIZE_BUCKET_NAME = " + Config.MAXIMUM_SIZE_BUCKET_NAME);
		}

		msgBuffer.position(QUANTITY_SIZE);
		CharBuffer cb = msgBuffer.asCharBuffer();
		cb.put(bucket);

		msgBuffer.position(QUANTITY_SIZE + Config.MAXIMUM_SIZE_BUCKET_NAME);
		ib = msgBuffer.asIntBuffer();
		ib.put(numMsgs);
		for (int i = 0; i < numMsgs; i++) {
			ib.put(offsets[i]);
		}
		return buff;
	}

	/**
	 * recovery to the initial status
	 */
	public void clear() {
		numMsgs = 0;
		msgWriteCursor = MSG_REGION_OFFSET;
		msgBuffer = ByteBuffer.wrap(buff);
	}
}

class ReadableSegment extends Segment {
	/**
	 * the segment byte offset in the super-segment
	 */
	int offsetInSuperSegment;

	/**
	 * segment byte length in the super-segment
	 */
	int segLength;
	MappedByteBuffer buffer;
	int readMsgIndex = 0; // from 0

	private ReadableSegment(MappedByteBuffer buffer, int offset, int length) {
		super();
		this.buffer = buffer;
		this.offsetInSuperSegment = offset;
		this.segLength = length;
		disassemble();
	}

	public static ReadableSegment wrap(MappedByteBuffer buffer, int offset, int length) {
		return new ReadableSegment(buffer, offset, length);
	}

	public Message read() throws SegmentEmptyException {
		if (readMsgIndex >= numMsgs) {
			throw new SegmentEmptyException();
		}
		// logger.info(
		// String.format("readMsgIndex = %d, offsetInSuperSegment = %d,
		// segLength = %d, offsets[readCursor] = %d",
		// readMsgIndex, offsetInSuperSegment, segLength,
		// offsets[readMsgIndex]));
		Message msg;
		try {
			msg = DefaultBytesMessage.deserializeToMsg(buffer, offsetInSuperSegment + offsets[readMsgIndex],
					segLength - offsets[readMsgIndex]);
		} catch (ByteMessageHeaderException e) {
			e.printStackTrace();
			return null;
		}
		readMsgIndex++;
		return msg;
	}

	// for message read
	public void disassemble() {
		// TODO optimize, compact
		buffer.position(offsetInSuperSegment);
		IntBuffer ib = buffer.asIntBuffer();
		int bucketNameLen = ib.get();

		buffer.position(offsetInSuperSegment + QUANTITY_SIZE);
		CharBuffer cb = buffer.asCharBuffer();

		char[] bucketChars = new char[bucketNameLen];
		cb.get(bucketChars);
		bucket = new String(bucketChars);

		buffer.position(offsetInSuperSegment + QUANTITY_SIZE + Config.MAXIMUM_SIZE_BUCKET_NAME);
		ib = buffer.asIntBuffer();
		numMsgs = ib.get();
		for (int i = 0; i < numMsgs; i++) {
			offsets[i] = ib.get();
		}
	}
}

class SegmentInputStream extends InputStream {

	ByteBuffer msgBuffer;

	SegmentInputStream(byte[] buff) {
		msgBuffer = ByteBuffer.wrap(buff, 0, buff.length);
	}

	@Override
	public int read() throws IOException {
		return msgBuffer.get();
	}

	@Override
	public int read(byte dst[], int off, int len) throws IOException {
		msgBuffer.get(dst, off, len);
		return len;
	}
}

class SegmentOutputStream extends OutputStream {
	ByteBuffer msgBuffer;

	SegmentOutputStream(ByteBuffer msgBuffer) {
		this.msgBuffer = msgBuffer;
	}

	SegmentOutputStream(byte[] buff) {
		msgBuffer = ByteBuffer.wrap(buff, 0, buff.length);
	}

	@Override
	public void write(int b) throws IOException {
		msgBuffer.put((byte) b);
	}

	@Override
	public void write(byte b[], int off, int len) throws IOException {
		msgBuffer.put(b, off, len);
	}
}

class MappedByteBufferInputStream extends InputStream {
	MappedByteBuffer buffer;

	MappedByteBufferInputStream(MappedByteBuffer buffer) {
		this.buffer = buffer;
	}

	public MappedByteBuffer getBuffer() {
		return buffer;
	}

	@Override
	public int read() throws IOException {
		return buffer.get();
	}

	@Override
	public int read(byte dst[], int off, int len) throws IOException {
		buffer.get(dst, off, len);
		return len;
	}
}
