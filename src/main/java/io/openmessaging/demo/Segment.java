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

	public final static int CAPACITY = Config.SEGMENT_SIZE;
	final int quantitySize = 4, offsetSize = 4;

	final int averageSizeOfMsg = 100;
	final int maxNumMsg = (CAPACITY - quantitySize * 2 - Config.MAXIMUM_SIZE_BUCKET_NAME) / (averageSizeOfMsg + 4);
	final int offsetRegionSize = maxNumMsg * 4;
	final int msgRegionSize = CAPACITY - quantitySize * 2 - Config.MAXIMUM_SIZE_BUCKET_NAME - offsetRegionSize;
	final int msgRegionOffset = Config.MAXIMUM_SIZE_BUCKET_NAME + quantitySize * 2 + offsetRegionSize;

	String bucket = null;
	int numMsgs = 0; // actual quantity

	/**
	 * msg byte offer in this segment
	 */
	int[] offsets;

	Segment() {
		offsets = new int[maxNumMsg];
	}
}

class WritableSegment extends Segment {
	byte[] buff;
	ByteBuffer msgBuffer;
	SegmentOutputStream segOutput;
	ObjectOutputStream msgOutput;

	WritableSegment(String bucket) {
		super();
		this.bucket = bucket;
		buff = new byte[CAPACITY];
		msgBuffer = ByteBuffer.wrap(buff, msgRegionOffset, msgRegionSize);
		segOutput = new SegmentOutputStream(msgBuffer);
		try {
			msgOutput = new ObjectOutputStream(segOutput);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void append(Message msg) throws SegmentFullException {
		int pos = msgBuffer.position();
		if (numMsgs >= maxNumMsg) {
			throw new SegmentFullException(
					"Segment meta is not enough, content remains " + (Segment.CAPACITY - pos) + " bytes");
		}
		msgBuffer.mark();
		try {
			msgOutput.writeObject(msg);
		} catch (BufferOverflowException e) {
			msgBuffer.reset();
			throw new SegmentFullException(
					"Segment content is not enough, meta remains " + (maxNumMsg - numMsgs) + " units");
		} catch (IOException e) {
			e.printStackTrace();
		}
		offsets[numMsgs++] = pos;
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

		msgBuffer.position(quantitySize);
		CharBuffer cb = msgBuffer.asCharBuffer();
		cb.put(bucket);

		msgBuffer.position(quantitySize + Config.MAXIMUM_SIZE_BUCKET_NAME);
		ib = msgBuffer.asIntBuffer();
		ib.put(numMsgs);
		for (int i = 0; i < offsets.length; i++) {
			ib.put(offsets[i]);
		}
		// byte[] header = new byte[4];
		// System.arraycopy(buff, msgRegionOffset, header, 0, 4);
		// System.out.println(Arrays.toString(header));
		return buff;
	}

	class SegmentOutputStream extends OutputStream {
		ByteBuffer msgBuffer;

		SegmentOutputStream(ByteBuffer msgBuffer) {
			this.msgBuffer = msgBuffer;
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

	/**
	 * recovery to the initial status
	 */
	public void clear() {
		numMsgs = 0;
		try {
			msgOutput.close();
			msgBuffer = ByteBuffer.wrap(buff, msgRegionOffset, msgRegionSize);
			segOutput = new SegmentOutputStream(msgBuffer);
			msgOutput = new ObjectOutputStream(segOutput);
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	MappedByteBufferInputStream mappedInput;
	ObjectInputStream msgInput;
	int readCursor = 0; // from 0

	private ReadableSegment(MappedByteBuffer buffer, int offset, int length) {
		super();
		this.buffer = buffer;
		this.offsetInSuperSegment = offset;
		this.segLength = length;
		mappedInput = new MappedByteBufferInputStream(buffer);
		disassemble();
		try {
			buffer.position(offsetInSuperSegment + msgRegionOffset);

			// byte[] header = new byte[4];
			// buffer.get(header);
			// System.out.println(Arrays.toString(header));
			buffer.position(offsetInSuperSegment + msgRegionOffset);
			msgInput = new ObjectInputStream(mappedInput);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static ReadableSegment wrap(MappedByteBuffer buffer, int offset, int length) {
		return new ReadableSegment(buffer, offset, length);
	}

	public Message read() throws SegmentEmptyException {
		if (readCursor >= numMsgs) {
			try {
				msgInput.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			throw new SegmentEmptyException();
		}
		Message msg = null;
		int pos = buffer.position();
		// logger.info(String.format("readCursor = %d, pos = %d, num = %d,
		// offsets[readCursor] = %d", readCursor, pos, num,
		// offsets[readCursor]));
		try {
			msg = (Message) msgInput.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// assert
		if (offsets[readCursor++] + offsetInSuperSegment != pos) {
			logger.severe(String.format("Offset meta is not consistent with the acutal message offsets, %d != %d",
					offsets[readCursor - 1] + offsetInSuperSegment, pos));
		}
		return msg;
	}

	// for message read
	public void disassemble() {
		// TODO optimize, compact
		buffer.position(offsetInSuperSegment);
		IntBuffer ib = buffer.asIntBuffer();
		int bucketNameLen = ib.get();

		buffer.position(offsetInSuperSegment + quantitySize);
		CharBuffer cb = buffer.asCharBuffer();

		char[] bucketChars = new char[bucketNameLen];
		cb.get(bucketChars);
		bucket = new String(bucketChars);

		buffer.position(offsetInSuperSegment + quantitySize + Config.MAXIMUM_SIZE_BUCKET_NAME);
		ib = buffer.asIntBuffer();
		numMsgs = ib.get();
		for (int i = 0; i < offsets.length; i++) {
			offsets[i] = ib.get();
		}
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
