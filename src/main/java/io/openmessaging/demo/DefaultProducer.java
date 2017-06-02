package io.openmessaging.demo;

import java.util.logging.Logger;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public class DefaultProducer implements Producer {
	private static Logger logger = Logger.getGlobal();

	private DefaultMessageFactory messageFactory = new DefaultMessageFactory();
	private SmartMessageStore messageStore;

	private KeyValue properties;

	public DefaultProducer(KeyValue properties) {
		this.properties = properties;
		SmartMessageStore.STORE_PATH = this.properties.getString("STORE_PATH");
		SmartMessageStore.IS_OUTPUT_OR_INPUT = true;
		BucketWriteBox.register(this);
	}

	@Override
	public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
		return messageFactory.createBytesMessageToTopic(topic, body);
	}

	@Override
	public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
		return messageFactory.createBytesMessageToQueue(queue, body);
	}

	@Override
	public void start() {

	}

	@Override
	public void shutdown() {

	}

	@Override
	public KeyValue properties() {
		return properties;
	}

	boolean isFirstSend = true;

	@Override
	public void send(Message message) {
		if (isFirstSend) {
			messageStore = SmartMessageStore.getInstance();
			isFirstSend = false;
		}
		if (message == null)
			throw new ClientOMSException("Message should not be null");
		String topic = message.headers().getString(MessageHeader.TOPIC);
		String queue = message.headers().getString(MessageHeader.QUEUE);
		if ((topic == null && queue == null) || (topic != null && queue != null)) {
			throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
		}

		messageStore.putMessage(topic != null ? topic : queue, this, message);
	}

	@Override
	public void send(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public Promise<Void> sendAsync(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void sendOneway(Message message, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
		throw new UnsupportedOperationException("Unsupported");
	}

	@Override
	public void flush() {
		messageStore.record(messageFactory.getQueues(), messageFactory.getTopics());
		messageStore.flush();

		// logger.info(String.format(
		// "maxMsgSize = %d, minMsgSize = %d, numMsg = %d, averageMsgSize = %d,
		// numMsgLess100 = %d, numMsgMore200 = %d",
		// messageFactory.maxMsgSize, messageFactory.minMsgSize,
		// messageFactory.numMsg,
		// messageFactory.numMsg == 0 ? 0 : messageFactory.totalMsgSize /
		// messageFactory.numMsg,
		// messageFactory.numMsgLess100, messageFactory.numMsgMore200));

		// logger.info(String.format(
		// "numHeaderInt = %d, numHeaderString = %d, numHeaderDouble =
		// %d,numHeaderLong = %d, numPropInt = %d, numPropString = %d,
		// numPropDouble = %d, numPropLong = %d",
		// DefaultBytesMessage.numHeaderInt.get(),
		// DefaultBytesMessage.numHeaderString.get(),
		// DefaultBytesMessage.numHeaderDouble.get(),
		// DefaultBytesMessage.numHeaderLong.get(),
		// DefaultBytesMessage.numPropInt.get(),
		// DefaultBytesMessage.numPropString.get(),
		// DefaultBytesMessage.numPropDouble.get(),
		// DefaultBytesMessage.numPropLong.get()));

		logger.info(String.format("maxKeySize = %d, maxValueSize = %d, maxKvSize = %d,maxKvNum = %d",
				DefaultBytesMessage.maxKeySize, DefaultBytesMessage.maxValueSize, DefaultBytesMessage.maxKvSize,
				DefaultBytesMessage.maxKvNum));

	}
}