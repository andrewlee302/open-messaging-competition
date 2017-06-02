package io.openmessaging.demo;

import java.nio.MappedByteBuffer;
import java.util.Map.Entry;
import java.util.logging.Logger;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

public class DefaultBytesMessage implements BytesMessage {

	private static final long serialVersionUID = 1955733544808061966L;
	private static transient Logger logger = Logger.getGlobal();

	private DefaultKeyValue headers;
	private DefaultKeyValue properties;
	private byte[] body;

	// transient static AtomicInteger numHeaderInt = new AtomicInteger();
	// transient static AtomicInteger numHeaderString = new AtomicInteger();
	// transient static AtomicInteger numHeaderDouble = new AtomicInteger();
	// transient static AtomicInteger numHeaderLong = new AtomicInteger();
	// transient static AtomicInteger numPropInt = new AtomicInteger();
	// transient static AtomicInteger numPropString = new AtomicInteger();
	// transient static AtomicInteger numPropDouble = new AtomicInteger();
	// transient static AtomicInteger numPropLong = new AtomicInteger();

	static transient int maxKeySize = 0;
	static transient int maxValueSize = 0;
	static transient int maxKvSize = 0;
	static transient int maxKvNum = 0;

	public DefaultBytesMessage(byte[] body) {
		this.body = body;
		headers = new DefaultKeyValue();
	}

	public DefaultBytesMessage(byte[] body, boolean initHeaders) {
		this.body = body;
		if (initHeaders)
			headers = new DefaultKeyValue();
	}

	/**
	 * int for total length, 2 byte for #header-kvs and #prop-kvs. We know all
	 * the value is String object (and ascii code), so we only care about the
	 * String value. key, value offset is 1 byte.
	 */
	static final int HEADER_SIZE = 6;
	transient int numHeaderKvs = 0;
	transient int numPropKvs = 0;
	transient int kvSize = 0;

	/**
	 * all offset is relative to the msg region, instead of the whole buff
	 * 
	 * @param buff
	 * @param off
	 *            start from off
	 * @param len
	 * @return msg size. if 0, failed.
	 */
	public int serializeToArray(byte[] buff, int off, int len) {
		if (kvSize > maxKvSize) {
			maxKvSize = kvSize;
		}
		if (numHeaderKvs + numPropKvs > maxKvNum) {
			maxKvNum = numHeaderKvs + numPropKvs;
		}

		int kvStart = HEADER_SIZE + (numHeaderKvs + numPropKvs) * 2 + 1;
		int bodyStart = kvStart + kvSize;
		int msgSize = bodyStart + body.length;
		// System.out.printf("kvStart = %d, bodyStart = %d, kvSize = %d,
		// bodyStart = %d, msgSize = %d\n", kvStart,
		// bodyStart, kvSize, bodyStart, msgSize);
		if (msgSize > len) {
			return 0;
		}
		byte[] temp = new byte[kvStart + kvSize];
		// big-endian
		temp[0] = (byte) (msgSize >> 24);
		temp[1] = (byte) (msgSize >> 16);
		temp[2] = (byte) (msgSize >> 8);
		temp[3] = (byte) msgSize;

		temp[4] = (byte) numHeaderKvs;
		temp[5] = (byte) numPropKvs;

		int kvOff = kvStart;
		int offPos = HEADER_SIZE;
		temp[offPos++] = (byte) kvOff;
		for (Entry<String, Object> entry : headers.kvs.entrySet()) {
			String key = entry.getKey();
			String value = (String) entry.getValue();
			key.getBytes(0, key.length(), temp, kvOff);
			kvOff += key.length();
			temp[offPos++] = (byte) kvOff;
			value.getBytes(0, value.length(), temp, kvOff);
			kvOff += value.length();
			temp[offPos++] = (byte) kvOff;
		}

		if (properties != null) {
			for (Entry<String, Object> entry : properties.kvs.entrySet()) {
				String key = entry.getKey();
				String value = (String) entry.getValue();
				key.getBytes(0, key.length(), temp, kvOff);
				kvOff += key.length();
				temp[offPos++] = (byte) kvOff;
				value.getBytes(0, value.length(), temp, kvOff);
				kvOff += value.length();
				temp[offPos++] = (byte) kvOff;
			}
		}

		System.arraycopy(temp, 0, buff, off, bodyStart);
		System.arraycopy(body, 0, buff, off + bodyStart, body.length);

		return msgSize;
	}

	public static DefaultBytesMessage deserializeToMsg(byte[] buff, int off, int len) {
		if (len < 4 || buff.length - off < 4) {
			return null;
		}
		int msgSize = (Byte.toUnsignedInt(buff[off]) << 24) + (Byte.toUnsignedInt(buff[off + 1]) << 16)
				+ (Byte.toUnsignedInt(buff[off + 2]) << 8) + buff[off + 3];
		if (len < msgSize || buff.length - off < msgSize) {
			return null;
		}

		byte[] temp = new byte[msgSize];
		System.arraycopy(buff, off, temp, 0, msgSize);
		return deserializeToMsg(temp);

	}

	public static DefaultBytesMessage deserializeToMsg(MappedByteBuffer buffer, int off, int len) {
		if (len < 4)
			return null;
		buffer.position(off);
		int msgSize = (Byte.toUnsignedInt(buffer.get()) << 24) + (Byte.toUnsignedInt(buffer.get()) << 16)
				+ (Byte.toUnsignedInt(buffer.get()) << 8) + buffer.get();
		if (len < msgSize) {
			return null;
		}

		byte[] temp = new byte[msgSize];
		buffer.position(off);
		buffer.get(temp);
		return deserializeToMsg(temp);
	}

	public static DefaultBytesMessage deserializeToMsg(byte[] msgBytes) {
		int msgSize = msgBytes.length;
		int numHeaderKvs = msgBytes[4];
		int numPropKvs = msgBytes[5];

		int offPos = 6;
		int kvOff = (int) msgBytes[offPos++];
		int nextKvOff = 0;
		DefaultKeyValue headers = new DefaultKeyValue();
		for (int i = 0; i < numHeaderKvs; i++) {
			nextKvOff = msgBytes[offPos++];
			// System.out.println(kvOff + ", " + nextKvOff);
			String key = new String(msgBytes, kvOff, nextKvOff - kvOff);
			kvOff = nextKvOff;
			nextKvOff = msgBytes[offPos++];
			// System.out.println(kvOff + ", " + nextKvOff);
			String value = new String(msgBytes, kvOff, nextKvOff - kvOff);
			kvOff = nextKvOff;
			headers.put(key, value);
		}

		DefaultKeyValue props = null;
		if (numPropKvs != 0) {
			props = new DefaultKeyValue();
			for (int i = 0; i < numHeaderKvs; i++) {
				nextKvOff = msgBytes[offPos++];
				String key = new String(msgBytes, kvOff, nextKvOff - kvOff);
				kvOff = nextKvOff;
				nextKvOff = msgBytes[offPos++];
				String value = new String(msgBytes, kvOff, nextKvOff - kvOff);
				kvOff = nextKvOff;
				props.put(key, value);
			}
		}

		int bodyStart = kvOff;
		int bodyLen = msgSize - bodyStart;
		// System.out.printf("bodyStart = %d, bodyStart = %d, bodyLen =
		// %d,msgSize = %d, offPos = %d\n", bodyStart,
		// bodyStart, bodyLen, msgSize, offPos);
		byte[] body = new byte[bodyLen];
		System.arraycopy(msgBytes, bodyStart, body, 0, bodyLen);
		DefaultBytesMessage msg = new DefaultBytesMessage(body, false);
		msg.headers = headers;
		msg.properties = props;

		return msg;
	}

	public static void main(String[] args) {
		final int bodyLen = 20000;
		byte[] body = new byte[bodyLen];
		DefaultBytesMessage msg = new DefaultBytesMessage(body);
		msg.putHeaders(MessageHeader.TOPIC, "TOPIC");
		msg.putHeaders(MessageHeader.MESSAGE_ID, "fd_fe");
		msg.putProperties("FFFFF", "b3bdj");
		msg.putProperties("fdfd3", "f3bdj");

		byte[] buff = new byte[1 << 20];

		if (msg.serializeToArray(buff, 100, 58 + bodyLen) == 0) {
			System.out.println("not enough space");
		}
		if (msg.serializeToArray(buff, 100, 59 + bodyLen) != 0) {
			System.out.println("ok");
			DefaultBytesMessage newMsg = DefaultBytesMessage.deserializeToMsg(buff, 100, 59 + bodyLen);
			byte[] msgBody = newMsg.getBody();
			// System.out.println(Arrays.toString(msgBody));
			if (msgBody.length == body.length) {
				System.out.println("body ok");
			}
			if (newMsg.headers.kvs.equals(msg.headers.kvs) && newMsg.properties.kvs.equals(msg.properties.kvs)) {
				System.out.println("header and props ok");

			}
		}
	}

	@Override
	public byte[] getBody() {
		return body;
	}

	@Override
	public BytesMessage setBody(byte[] body) {
		this.body = body;
		return this;
	}

	@Override
	public KeyValue headers() {
		return headers;
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	@Override
	public Message putHeaders(String key, int value) {
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, long value) {
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, double value) {
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, String value) {
		numHeaderKvs++;
		if (key.length() > maxKeySize)
			maxKeySize = key.length();
		if (value.length() > maxValueSize)
			maxValueSize = value.length();
		kvSize += (key.length() + value.length());
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, int value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, long value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, double value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, String value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		if (key.length() > maxKeySize)
			maxKeySize = key.length();
		if (value.length() > maxValueSize)
			maxValueSize = value.length();
		numPropKvs++;
		kvSize += (key.length() + value.length());
		properties.put(key, value);
		return this;
	}

	// TODO delete
	// transient String bucket;
	// transient int producerId, buekcetId, seq;
	// public void extract() {
	// String topic = headers().getString(MessageHeader.TOPIC);
	// String queue = headers().getString(MessageHeader.QUEUE);
	// byte[] body = getBody();
	// producerId = (int) body[0];
	// buekcetId = (int) body[1];
	// seq = StressProducerTester.unpack(body);
	//
	// // 实际测试时，会一一比较各个字段
	// if (topic != null) {
	// bucket = topic;
	// } else {
	// bucket = queue;
	// }
	// }
}
