package io.openmessaging.demo;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
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


	transient static AtomicInteger numHeaderInt = new AtomicInteger();
	transient static AtomicInteger numHeaderString = new AtomicInteger();
	transient static AtomicInteger numHeaderDouble = new AtomicInteger();
	transient static AtomicInteger numHeaderLong = new AtomicInteger();
	transient static AtomicInteger numPropInt = new AtomicInteger();
	transient static AtomicInteger numPropString = new AtomicInteger();
	transient static AtomicInteger numPropDouble = new AtomicInteger();
	transient static AtomicInteger numPropLong = new AtomicInteger();

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
		numHeaderInt.incrementAndGet();
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, long value) {
		numHeaderLong.incrementAndGet();
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, double value) {
		numHeaderDouble.incrementAndGet();
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putHeaders(String key, String value) {
		numHeaderString.incrementAndGet();
		headers.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, int value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		numPropInt.incrementAndGet();	
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, long value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		numPropLong.incrementAndGet();	
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, double value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		numPropDouble.incrementAndGet();	
		properties.put(key, value);
		return this;
	}

	@Override
	public Message putProperties(String key, String value) {
		if (properties == null)
			properties = new DefaultKeyValue();
		numPropString.incrementAndGet();	
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
