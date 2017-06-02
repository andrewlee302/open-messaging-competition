package io.openmessaging.demo;

import java.io.Serializable;
import java.util.logging.Logger;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage, Serializable {

	private static final long serialVersionUID = 1955733544808061966L;
	private static transient Logger logger = Logger.getGlobal();

	// private KeyValue headers = new DefaultHeaderKeyValue();
	private KeyValue headers = new DefaultKeyValue();
	private KeyValue properties;
	private byte[] body;

	transient static int numHeaders = 0;
	transient static int numProps = 0;

	public DefaultBytesMessage(byte[] body) {
		this.body = body;
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
		if (numHeaders % 100000 == 0) {
			logger.info(String.format("head int key = %s, value = %d", key, value));
		}
		numHeaders++;
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, long value) {
		if (numHeaders % 100000 == 0) {
			logger.info(String.format("head long key = %s, value = %d", key, value));
		}
		numHeaders++;
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, double value) {
		if (numHeaders % 100000 == 0) {
			logger.info(String.format("head double key = %s, value = %f", key, value));
		}
		numHeaders++;
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, String value) {
		if (numHeaders % 1000000 == 0) {
			logger.info(String.format("head string key = %s, value = %s", key, value));
		}
		numHeaders++;
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, int value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		if (numProps % 100000 == 0) {
			logger.info(String.format("prop int key = %s, value = %d", key, value));
		}
		numProps++;
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, long value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		if (numProps % 100000 == 0) {
			logger.info(String.format("prop long key = %s, value = %s", key, value));
		}
		numProps++;
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, double value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		if (numProps % 100000 == 0) {
			logger.info(String.format("prop double key = %s, value = %d", key, value));
		}
		numProps++;
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, String value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		if (numProps % 100000 == 0) {
			logger.info(String.format("prop string key = %s, value = %s", key, value));
		}
		numProps++;
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
