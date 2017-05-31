package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.Assert;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

public class StressConsumerTester extends StressTester {
	public static void main(String[] args) {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
		logger.info("abc");
		if (args.length < 1) {
			System.err.println("Args missing");
			System.err.println("<store_path> <num_msgs>");
			System.exit(1);
		} else if (args.length >= 1) {
			store_path = args[0];
			if (args.length >= 2)
				num_msgs = Integer.parseInt(args[1]);
		}
		System.out.println("----------------------------------");
		System.out.println(String.format("Launch process, num_msgs=%d", num_msgs));
		KeyValue properties = new DefaultKeyValue();
		/*
		 * //实际测试时利用 STORE_PATH 传入存储路径
		 * //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
		 */
		properties.put("STORE_PATH", store_path);

		// 这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

		// producers = new Producer[numProducers];
		// for (int i = 0; i < numProducers; i++) {
		// producers[i] = new DefaultProducer(properties);
		// }

		topics = new String[numTopics];
		for (int i = 0; i < numTopics; i++) {
			topics[i] = "TOPIC" + i;
		}

		consumers = new PullConsumer[numConsumers];
		queues = new String[numConsumers];
		for (int i = 0; i < numConsumers; i++) {
			queues[i] = "QUEUE" + i;
			consumers[i] = new DefaultPullConsumer(properties);
		}

		numSendMsgs = new AtomicInteger[numBuckets];
		numPullMsgs = new AtomicInteger[numBuckets];
		for (int i = 0; i < numBuckets; i++) {
			numSendMsgs[i] = new AtomicInteger();
			numPullMsgs[i] = new AtomicInteger();
		}

		// CountDownLatch sendDoneSignal = new CountDownLatch(numProducers);
		// input_threads = new Thread[numProducers];
		// for (int i = 0; i < numProducers; i++) {
		// final int ii = i;
		// input_threads[i] = new Thread(new Runnable() {
		// @Override
		// public void run() {
		// final Producer p = producers[ii];
		// Random rand = new Random(System.currentTimeMillis());
		//
		// int[] seqs = new int[numBuckets];
		// Message msg = null;
		// while (!Thread.currentThread().isInterrupted()) {
		// if (totalNumSendMsgs.getAndIncrement() >= num_msgs) {
		// break;
		// }
		// int buekcetId = rand.nextInt(numBuckets);
		// String bucket = null;
		//
		// // auto-incremental field of message
		//
		// if (buekcetId < numConsumers) {
		// bucket = queues[buekcetId];
		// msg = p.createBytesMessageToQueue(bucket, pack(ii, buekcetId,
		// seqs[buekcetId]++));
		// } else {
		// bucket = topics[buekcetId - numConsumers];
		// msg = p.createBytesMessageToTopic(bucket, pack(ii, buekcetId,
		// seqs[buekcetId]++));
		// }
		//
		// logger.info(Thread.currentThread().getName() + " send msg " +
		// totalNumSendMsgs.get());
		// p.send(msg);
		// }
		// for (int i = 0; i < numBuckets; i++) {
		// numSendMsgs[i].addAndGet(seqs[i]);
		// }
		// p.flush();
		// sendDoneSignal.countDown();
		// }
		// });
		// }
		//
		// System.out.println("Start produce");
		// long start = System.currentTimeMillis();
		// for (int i = 0; i < numProducers; i++) {
		// input_threads[i].start();
		// }
		//
		// try {
		// if (!sendDoneSignal.await(5 * 60, TimeUnit.SECONDS)) {
		// for (int i = 0; i < numProducers; i++) {
		// input_threads[i].interrupt();
		// }
		// }
		// for (int i = 0; i < numProducers; i++) {
		// input_threads[i].join();
		// }
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// long end = System.currentTimeMillis();
		// long T1 = end - start;
		// System.out.println(String.format("Produce cost:%d ms, send:%d q,
		// tps:%d", T1, totalNumSendMsgs.get(),
		// totalNumSendMsgs.get() * 1000 / T1));

		CountDownLatch pullDoneSignal = new CountDownLatch(numProducers);
		output_threads = new Thread[numConsumers];
		for (int i = 0; i < numProducers; i++) {
			final int ii = i;
			output_threads[i] = new Thread(new Runnable() {

				@Override
				public void run() {
					Random rand = new Random(System.currentTimeMillis());
					int numSubTopic = rand.nextInt(numTopics / 10 + 1);
					Set<String> subTopicSet = new HashSet<>();
					int cnt = 0;
					while (cnt < numSubTopic)
						if (subTopicSet.add(topics[rand.nextInt(numTopics)]))
							cnt++;

					final PullConsumer c = consumers[ii];
					final String bindQueue = queues[ii];
					ArrayList<String> subTopics = new ArrayList<>();
					subTopics.addAll(subTopicSet);
					c.attachQueue(bindQueue, subTopics);

					int[][] producerSeqs = new int[numProducers][numBuckets];
					while (true) {
						Message msg = c.poll();
						if (msg == null) {
							System.out.println(Thread.currentThread().toString() + " consumer ended");
							// 拉取为null则认为消息已经拉取完毕
							break;
						}
						DefaultBytesMessage byteMsg = (DefaultBytesMessage) msg;
						int localTotalNumPullMsgs = totalNumPullMsgs.incrementAndGet();

						String bucket = null;
						String topic = byteMsg.headers().getString(MessageHeader.TOPIC);
						String queue = byteMsg.headers().getString(MessageHeader.QUEUE);
						byte[] body = byteMsg.getBody();
						int producerId = (int) body[0];
						int buekcetId = (int) body[1];
						int seq = unpack(body);

						// 实际测试时，会一一比较各个字段
						if (topic != null) {
							bucket = topic;
							Assert.assertTrue(subTopicSet.contains(topic));
						} else {
							bucket = queue;
							Assert.assertEquals(bindQueue, queue);
						}
						// logger.info(Thread.currentThread().getName() + " poll
						// msg bucket: " + bucket
						// + " ,total msgs num " + localTotalNumPullMsgs);
						Assert.assertEquals(producerSeqs[producerId][buekcetId]++, seq);
					}
					pullDoneSignal.countDown();
//					StringBuffer sb = new StringBuffer(400);
//					for (int b = 0; b < numBuckets; b++) {
//						int bucketCap = 0;
//						for (int i = 0; i < numProducers; i++) {
//							bucketCap += producerSeqs[i][b];
//						}
//						sb.append(String.format("%4d", bucketCap) + " ");
//					}
//					System.out.println(sb.toString());
				}
			});
		}

		System.out.println("Start consume");

		long startConsumer = System.currentTimeMillis();
		for (int i = 0; i < numConsumers; i++) {
			output_threads[i].start();
		}
		try {
			pullDoneSignal.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long endConsumer = System.currentTimeMillis();
		long T2 = endConsumer - startConsumer;
		System.out.println(String.format("Pull cost:%d ms, pull:%d q, tps:%d ", T2, totalNumPullMsgs.get(),
				totalNumPullMsgs.get() * 1000 / (T2)));
		// System.out.println(String.format("Pull cost:%d ms, pull:%d q, tps:%d
		// ", T2 + T1, totalNumPullMsgs.get(),
		// totalNumPullMsgs.get() * 1000 / (T1 + T2)));

	}

	public static byte[] pack(int producerId, int bucketId, int seq) {
		int seqtmp = seq;
		byte[] body = new byte[6];
		body[0] = (byte) producerId;
		body[1] = (byte) bucketId;
		for (int i = 2; i < 6; i++) {
			body[i] = (byte) seq;
			seq >>>= 8;
		}
		// System.out.println(seqtmp + "\n" + Arrays.toString(body));
		return body;
	}

	public static int unpack(byte[] body) {
		int seq = 0;
		for (int i = 5; i > 1; i--) {
			seq <<= 8;
			seq |= (body[i] & 0xff);
		}
		return seq;
	}
}
