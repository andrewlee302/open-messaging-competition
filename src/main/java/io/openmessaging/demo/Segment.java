package io.openmessaging.demo;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

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

	}

	Segment(boolean init) {
		if (init)
			offsets = new int[MAX_NUM_MSG];
	}
}

class WritableSegment extends Segment {
	byte[] buff;
	ByteBuffer msgBuffer;
	int msgWriteCursor = MSG_REGION_OFFSET;

	WritableSegment(String bucket) {
		super(true);
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

	public static final ReadableSegment NULL = new ReadableSegment();
	/**
	 * the segment byte offset in the super-segment
	 */
	int offsetInSuperSegment;

	/**
	 * segment byte length in the super-segment
	 */
	int segLength;
	ByteBuffer buffer;
	int readMsgIndex = 0; // from 0

	private ReadableSegment() {

	}

	private ReadableSegment(ByteBuffer buffer, int offset, int length) {
		super(true);
		this.buffer = buffer;
		this.offsetInSuperSegment = offset;
		this.segLength = length;
		disassemble();
	}

	public static ReadableSegment wrap(ByteBuffer buffer, int offset, int length) {
		return new ReadableSegment(buffer, offset, length);
	}

	public static ReadableSegment wrap(byte[] buff, int offset, int length) {
		ByteBuffer buffer = ByteBuffer.wrap(buff, offset, length);
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
		return (msgBuffer.get() & 0xff);
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

class ByteBufferInputStream extends InputStream {
	ByteBuffer buffer;

	ByteBufferInputStream(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	public ByteBuffer getBuffer() {
		return buffer;
	}

	@Override
	public int read() throws IOException {
		return (buffer.get() & 0xff);
	}

	@Override
	public int read(byte dst[], int off, int len) throws IOException {
		int remain = buffer.remaining();
		if (remain <= 0)
			return -1;
		if (len > remain) {
			len = remain;
		}
		buffer.get(dst, off, len);
		return len;
	}
}

class CompressedSuperSegment extends SegmentOutputStream {
	GZIPOutputStream compressOutput;

	int numSeg = 0;
	int writeSize = 0;

	public CompressedSuperSegment(byte[] buff) {
		super(buff);
		try {
			compressOutput = new GZIPOutputStream(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	byte[] b;

	public void append(WritableSegment seg) {
		try {
			byte[] a = seg.assemble();
			compressOutput.write(a);
			writeSize += a.length;
			numSeg++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static int i = 0;

	/**
	 * the new copy byte array
	 * 
	 * @return
	 */
	public byte[] getCompressedData() {
		try {
			compressOutput.finish();
			compressOutput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		byte[] data = new byte[msgBuffer.position()];
		msgBuffer.flip();
		msgBuffer.get(data);

		FileOutputStream out;
		try {
			out = new FileOutputStream("/Users/andrew/workspace/java/open-messaging-demo/" + (i++) + ".txt");
			out.write(data);
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// ByteArrayInputStream bais = new ByteArrayInputStream(data);
		// GZIPInputStream decompressInput;
		// try {
		// decompressInput = new GZIPInputStream(bais);
		//
		// // TODO delete
		// byte[] origin = new byte[numSeg * Segment.CAPACITY];
		// try {
		// int len;
		// int off = 0;
		// while ((len = decompressInput.read(origin, off, origin.length - off))
		// > 0) {
		// off += len;
		// }
		// System.out.printf("secretSize=%d, deCompressSize=%d,
		// expectedSize=%d\n", data.length, off, writeSize);
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// } catch (IOException e1) {
		// e1.printStackTrace();
		// }

		return data;
	}
}

class DecompressedSuperSegment extends ByteBufferInputStream {
	GZIPInputStream decompressInput;
	int numSegs;
	int originSuperSegSize;
	long compressedSize;

	DecompressedSuperSegment(ByteBuffer buffer, int numSegs, long compressedSize) {
		super(buffer);
		this.numSegs = numSegs;
		this.originSuperSegSize = numSegs * Config.SEGMENT_SIZE;
		this.compressedSize = compressedSize;
		try {
			decompressInput = new GZIPInputStream(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public byte[] decompress() {
		byte[] data = new byte[originSuperSegSize];
		try {
			int len, off = 0;
			while ((len = decompressInput.read(data, off, originSuperSegSize - off)) > 0) {
				off += len;
			}
			if (off != originSuperSegSize) {
				System.out.println("Exception server error");
			}
			decompressInput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}

}
