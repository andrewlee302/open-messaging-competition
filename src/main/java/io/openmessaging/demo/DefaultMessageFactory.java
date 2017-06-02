package io.openmessaging.demo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

public class DefaultMessageFactory implements MessageFactory {

	// TODO
	transient static HashMap<Byte, String> byteToBucketMap;

	public static byte transferToByte(String bucket) {
		return (byte) bucket.hashCode();
	}

	private Set<String> queues;
	private Set<String> topics;

	int maxMsgSize;
	int minMsgSize;
	long totalMsgSize;
	long numMsg;
	long numMsgLess100;
	long numMsgMore200;

	public DefaultMessageFactory() {
		byteToBucketMap = new HashMap<>(100);
		queues = new HashSet<>();
		topics = new HashSet<>();
	}

	@Override
	public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
		int len = body.length;
		if (len > maxMsgSize)
			maxMsgSize = len;
		if (len < minMsgSize)
			minMsgSize = len;
		if (len < 100)
			numMsgLess100++;
		if (len > 200)
			numMsgMore200++;
		numMsg++;
		totalMsgSize += len;

		topics.add(topic);
		DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
		defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
		return defaultBytesMessage;
	}

	@Override
	public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
		int len = body.length;
		if (len > maxMsgSize)
			maxMsgSize = len;
		if (len < minMsgSize)
			minMsgSize = len;
		if (len < 100)
			numMsgLess100++;
		if (len > 200)
			numMsgMore200++;
		numMsg++;
		totalMsgSize += len;

		queues.add(queue);
		DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
		defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
		return defaultBytesMessage;
	}

	public Set<String> getQueues() {
		return queues;
	}

	public Set<String> getTopics() {
		return topics;
	}
}
