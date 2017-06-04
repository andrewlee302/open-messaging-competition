package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
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
	
	static AtomicLong totalMsgSize = new AtomicLong();

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
	 * 1byte for msg HEADER identify (0xFF) int for total length, 2 byte for
	 * #header-kvs and #prop-kvs. We know all the value is String object (and
	 * ascii code), so we only care about the String value. key, value offset is
	 * 1 byte.
	 * 
	 * Replace strings to one byte as follows: Topic -> 100 Queue -> 101
	 * MessageId -> 102 PRO_OFFSET -> 103 TOPIC_X -> X QUEUE_X -> 90+X
	 */
	static final int HEADER_SIZE = 7;
	transient int numHeaderKvs = 0;
	transient int numPropKvs = 0;
	transient int kvSize = 0;

	public static HashMap<String, Byte> replaceTable = new HashMap<>();
	public static HashMap<Byte, String> replaceTableReverse = new HashMap<>();

	static {
		replaceTable.put(MessageHeader.TOPIC, (byte) 100);
		replaceTable.put(MessageHeader.QUEUE, (byte) 101);
		replaceTable.put(MessageHeader.MESSAGE_ID, (byte) 102);
		replaceTable.put("PRO_OFFSET", (byte) 103);
		for (String topic : Config.HACK_TOPICS) {
			replaceTable.put(topic, Byte.parseByte(topic.substring(6)));
		}
		for (String queue : Config.HACK_QUEUES) {
			replaceTable.put(queue, (byte) (Byte.parseByte(queue.substring(6)) + 90));
		}

		replaceTableReverse.put((byte) 100, MessageHeader.TOPIC);
		replaceTableReverse.put((byte) 101, MessageHeader.QUEUE);
		replaceTableReverse.put((byte) 102, MessageHeader.MESSAGE_ID);
		replaceTableReverse.put((byte) 103, "PRO_OFFSET");
		for (String topic : Config.HACK_TOPICS) {
			replaceTableReverse.put(Byte.parseByte(topic.substring(6)), topic);
		}

		for (String queue : Config.HACK_QUEUES) {
			replaceTableReverse.put((byte) (Byte.parseByte(queue.substring(6)) + 90), queue);
		}
	}
	static final byte HEADER = (byte) 0xFF;

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
		if (len < HEADER_SIZE) {
			return 0;
		}

		buff[off] = HEADER;

		// relative to the msg zero position
		int kvStart = HEADER_SIZE + (numHeaderKvs + numPropKvs) * 4 + 2;
		int bodyStart = kvStart + kvSize;
		int msgSize = bodyStart + body.length;
		// logger.info(String.format("kvStart = %d, bodyStart = %d, kvSize = %d,
		// bodySize = %d, msgSize = %d\n", kvStart,
		// bodyStart, kvSize, body.length, msgSize));
		if (msgSize > len) {
			return 0;
		}
		_int24byte(msgSize, buff, off + 1);
		buff[off + 5] = (byte) numHeaderKvs;
		buff[off + 6] = (byte) numPropKvs;

		int kvOff = kvStart;
		int offPos = HEADER_SIZE;
		_int22byte(kvOff, buff, off + offPos);
		offPos += 2;
		for (Entry<String, Object> entry : headers.kvs.entrySet()) {
			String key = entry.getKey();
			String value = (String) entry.getValue();

			Byte kb = replaceTable.get(key);
			if (kb != null) {
				buff[off + kvOff] = kb.byteValue();
				kvOff += 1;
			} else {
				key.getBytes(0, key.length(), buff, off + kvOff);
				kvOff += key.length();
			}
			_int22byte(kvOff, buff, off + offPos);
			offPos += 2;

			Byte vb = replaceTable.get(value);
			if (vb != null) {
				buff[off + kvOff] = vb.byteValue();
				kvOff += 1;
			} else {
				value.getBytes(0, value.length(), buff, off + kvOff);
				kvOff += value.length();
			}
			_int22byte(kvOff, buff, off + offPos);
			offPos += 2;
		}

		if (properties != null) {
			for (Entry<String, Object> entry : properties.kvs.entrySet()) {
				String key = entry.getKey();
				String value = (String) entry.getValue();

				Byte kb = replaceTable.get(key);
				if (kb != null) {
					buff[off + kvOff] = kb;
					kvOff += 1;
				} else {
					key.getBytes(0, key.length(), buff, off + kvOff);
					kvOff += key.length();
				}
				_int22byte(kvOff, buff, off + offPos);
				offPos += 2;

				Byte vb = replaceTable.get(value);
				if (vb != null) {
					buff[off + kvOff] = vb;
					kvOff += 1;
				} else {
					value.getBytes(0, value.length(), buff, off + kvOff);
					kvOff += value.length();
				}
				_int22byte(kvOff, buff, off + offPos);
				offPos += 2;
			}
		}

		System.arraycopy(body, 0, buff, off + bodyStart, body.length);
		// byte[] cc = new byte[bodyStart];
		// System.arraycopy(buff, off, cc, 0, bodyStart);
		// System.out.println(Arrays.toString(cc));
		
		totalMsgSize.addAndGet(msgSize);
		return msgSize;
	}

	public static DefaultBytesMessage deserializeToMsg(byte[] buff, int off, int len)
			throws ByteMessageHeaderException {
		if (buff[off] != HEADER) {
			throw new ByteMessageHeaderException();
		}

		if (len < 5 || buff.length - off < 5) {
			return null;
		}
		int msgSize = _4byte2int(buff, off + 1);
		if (len < msgSize || buff.length - off < msgSize) {
			return null;
		}

		byte[] temp = new byte[msgSize];
		System.arraycopy(buff, off, temp, 0, msgSize);
		return deserializeToMsg(temp);

	}

	public static DefaultBytesMessage deserializeToMsg(ByteBuffer buffer, int off, int len)
			throws ByteMessageHeaderException {
		buffer.position(off);
		if (buffer.get() != HEADER) {
			throw new ByteMessageHeaderException();
		}
		if (len < 5)
			return null;
		int msgSize = _4byte2int(buffer, off + 1);
		if (len < msgSize) {
			logger.warning(String.format("%d < %d", len, msgSize));
			return null;
		}

		byte[] temp = new byte[msgSize];
		buffer.position(off);
		buffer.get(temp);
		buffer = null;
		return deserializeToMsg(temp);
	}

	public static DefaultBytesMessage deserializeToMsg(byte[] msgBytes) {
		int msgSize = msgBytes.length;
		int numHeaderKvs = msgBytes[5];
		int numPropKvs = msgBytes[6];

		int offPos = HEADER_SIZE;
		int kvOff = _2byte2int(msgBytes, offPos);
		offPos += 2;
		int nextKvOff = 0;
		DefaultKeyValue headers = new DefaultKeyValue();
		for (int i = 0; i < numHeaderKvs; i++) {
			String key, value;

			nextKvOff = _2byte2int(msgBytes, offPos);
			offPos += 2;

			int s_size = nextKvOff - kvOff;
			if (s_size == 1) {
				key = replaceTableReverse.get(msgBytes[kvOff]);
			} else {
				key = new String(msgBytes, kvOff, s_size);
			}
			kvOff = nextKvOff;
			nextKvOff = _2byte2int(msgBytes, offPos);
			offPos += 2;

			s_size = nextKvOff - kvOff;
			if (s_size == 1) {
				value = replaceTableReverse.get(msgBytes[kvOff]);
			} else {
				value = new String(msgBytes, kvOff, s_size);
			}
			kvOff = nextKvOff;

			headers.put(key, value);
		}

		DefaultKeyValue props = null;
		if (numPropKvs != 0) {
			props = new DefaultKeyValue();
			for (int i = 0; i < numPropKvs; i++) {
				String key, value;

				nextKvOff = _2byte2int(msgBytes, offPos);
				offPos += 2;

				int s_size = nextKvOff - kvOff;
				if (s_size == 1) {
					key = replaceTableReverse.get(msgBytes[kvOff]);
				} else {
					key = new String(msgBytes, kvOff, s_size);
				}
				kvOff = nextKvOff;
				nextKvOff = _2byte2int(msgBytes, offPos);
				offPos += 2;

				s_size = nextKvOff - kvOff;
				if (s_size == 1) {
					value = replaceTableReverse.get(msgBytes[kvOff]);
				} else {
					value = new String(msgBytes, kvOff, s_size);
				}
				kvOff = nextKvOff;
				props.put(key, value);
			}
		}

		int bodyStart = kvOff;
		int bodyLen = msgSize - bodyStart;
		// logger.info(String.format("bodyStart = %d, bodyLen = %d,msgSize = %d,
		// offPos = %d\n", bodyStart, bodyLen,
		// msgSize, offPos));
		byte[] body = new byte[bodyLen];
		System.arraycopy(msgBytes, bodyStart, body, 0, bodyLen);
		DefaultBytesMessage msg = new DefaultBytesMessage(body, false);
		msg.headers = headers;
		msg.properties = props;
		return msg;
	}

	public static int _2byte2int(byte[] buff, int offset) {
		// big-endian
		int num = (Byte.toUnsignedInt(buff[offset]) << 8) + (Byte.toUnsignedInt(buff[offset + 1]));
		return num;
	}

	public static void _int22byte(int num, byte[] buff, int offset) {
		// big-endian
		buff[offset] = (byte) (num >> 8);
		buff[offset + 1] = (byte) num;
	}

	public static int _4byte2int(byte[] buff, int offset) {
		int num = (Byte.toUnsignedInt(buff[offset]) << 24) + (Byte.toUnsignedInt(buff[offset + 1]) << 16)
				+ (Byte.toUnsignedInt(buff[offset + 2]) << 8) + Byte.toUnsignedInt(buff[offset + 3]);
		return num;
	}

	public static int _4byte2int(ByteBuffer buffer, int offset) {
		buffer.position(offset);
		int num = (Byte.toUnsignedInt(buffer.get()) << 24) + (Byte.toUnsignedInt(buffer.get()) << 16)
				+ (Byte.toUnsignedInt(buffer.get()) << 8) + Byte.toUnsignedInt(buffer.get());

		return num;
	}

	public static void _int24byte(int num, byte[] buff, int offset) {
		// big-endian
		buff[offset] = (byte) (num >> 24);
		buff[offset + 1] = (byte) (num >> 16);
		buff[offset + 2] = (byte) (num >> 8);
		buff[offset + 3] = (byte) num;
	}

	public static void main(String[] args) throws ByteMessageHeaderException {
		final int bodyLen = 20000;
		byte[] body = new byte[bodyLen];
		DefaultBytesMessage msg = new DefaultBytesMessage(body);
		msg.putHeaders(MessageHeader.TOPIC, "TOPIC_3");
		msg.putHeaders(MessageHeader.MESSAGE_ID, "fd_fe");
		msg.putProperties("FFFFF", "b3bdj");
		msg.putProperties("fdfd3", "f3bdj");
		StringBuffer sb = new StringBuffer(2000);
		for (int i = 0; i < 2000 / 4; i++) {
			sb.append("abcd");
		}
		msg.putProperties("fdffd", sb.toString());

		byte[] buff = new byte[1 << 20];

		if (msg.serializeToArray(buff, 100, 2061 + bodyLen) == 0) {
			System.out.println("not enough space");
		}
		if (msg.serializeToArray(buff, 100, 2062 + bodyLen) != 0) {
			System.out.println("ok");
			DefaultBytesMessage newMsg = DefaultBytesMessage.deserializeToMsg(buff, 100, 2062 + +bodyLen);
			byte[] msgBody = newMsg.getBody();
			System.out.println(Arrays.toString(msgBody));
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
		
		if (replaceTable.containsKey(key)) {
			kvSize++;
		} else {
			kvSize += key.length();
		}
		if (replaceTable.containsKey(value)) {
			kvSize++;
		} else {
			kvSize += value.length();
		}
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
		numPropKvs++;
		if (properties == null)
			properties = new DefaultKeyValue();

		if (replaceTable.containsKey(key)) {
			kvSize++;
		} else {
			kvSize += key.length();
		}
		if (replaceTable.containsKey(value)) {
			kvSize++;
		} else {
			kvSize += value.length();
		}
		properties.put(key, value);
		return this;
	}
}

class ByteMessageHeaderException extends Exception {

	private static final long serialVersionUID = 2795554105942132999L;

}