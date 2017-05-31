package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
	private SmartMessageStore messageStore;
	private KeyValue properties;
	private String queue;
	private Set<String> buckets = new HashSet<>();

	// bucketList 包含了queue和topics，不区分对待。
	// 在消费的时候，除了要读取绑定的Topic的数据，还要去取直接发送到该Queue的数据
	private List<String> bucketList = new ArrayList<>();

	private int lastIndex = 0;

	public DefaultPullConsumer(KeyValue properties) {
		this.properties = properties;
		SmartMessageStore.STORE_PATH = this.properties.getString("STORE_PATH");
		SmartMessageStore.IS_OUTPUT_OR_INPUT = false;
		messageStore = SmartMessageStore.getInstance();
	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	@Override
	public synchronized Message poll() {
		if (buckets.size() == 0 || queue == null) {
			return null;
		}
		return messageStore.pullMessage(queue, bucketList.size());

	}

	@Override
	public Message poll(KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void ack(String messageId, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public synchronized void attachQueue(String queueName, Collection<String> topics) {
		if (queue != null && !queue.equals(queueName)) {
			throw new ClientOMSException("You have alreadly attached to a queue " + queue);
		}
		queue = queueName;
		buckets.add(queueName);
		buckets.addAll(topics);
		bucketList.clear();
		bucketList.addAll(buckets);
		messageStore.register(queueName, bucketList);
	}

}
