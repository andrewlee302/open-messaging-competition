package io.openmessaging.demo;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

class StressTester {
	protected static Logger logger = Logger.getGlobal();
	protected static int num_msgs = 400000;

	protected static String store_path;
	protected static int numProducers = 10;
	protected static Producer[] producers;
	protected static Thread[] input_threads;

	protected static int numConsumers = 10;
	protected static PullConsumer[] consumers;
	protected static String[] queues;
	protected static Thread[] output_threads;

	protected static int numTopics = 90;
	protected static String[] topics;

	protected static final int numBuckets = numConsumers + numTopics;
	protected static AtomicInteger totalNumSendMsgs = new AtomicInteger();
	protected static AtomicInteger totalNumPullMsgs = new AtomicInteger();
	protected static AtomicInteger[] numSendMsgs;
	protected static AtomicInteger[] numPullMsgs;
}

public class StressProducerTester extends StressTester {

	public static void main(String[] args) {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");
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

		producers = new Producer[numProducers];
		for (int i = 0; i < numProducers; i++) {
			producers[i] = new DefaultProducer(properties);
		}

		topics = new String[numTopics];
		for (int i = 0; i < numTopics; i++) {
			topics[i] = "TOPIC_" + i;
		}

		queues = new String[numConsumers];
		for (int i = 0; i < numConsumers; i++) {
			queues[i] = "QUEUE_" + i;
		}

		numSendMsgs = new AtomicInteger[numBuckets];
		numPullMsgs = new AtomicInteger[numBuckets];
		for (int i = 0; i < numBuckets; i++) {
			numSendMsgs[i] = new AtomicInteger();
			numPullMsgs[i] = new AtomicInteger();

		}

		CountDownLatch sendDoneSignal = new CountDownLatch(numProducers);
		output_threads = new Thread[numProducers];
		for (int i = 0; i < numProducers; i++) {
			final int ii = i;
			output_threads[i] = new Thread(new Runnable() {
				@Override
				public void run() {
					final Producer p = producers[ii];
					Random rand = new Random(System.nanoTime());

					int[] seqs = new int[numBuckets];
					Message msg = null;
					while (true) {
						int localTotalNum = totalNumSendMsgs.get();
						if (localTotalNum >= num_msgs) {
							break;
						}
						totalNumSendMsgs.incrementAndGet();
						int buekcetId = rand.nextInt(numBuckets);
						String bucket = null;

						// auto-incremental field of message

						if (buekcetId < numConsumers) {
							bucket = queues[buekcetId];
							byte[] body = pack(ii, buekcetId, seqs[buekcetId]++);
							msg = p.createBytesMessageToQueue(bucket, body);
						} else {
							bucket = topics[buekcetId - numConsumers];
							byte[] body = pack(ii, buekcetId, seqs[buekcetId]++);
							msg = p.createBytesMessageToTopic(bucket, body);
						}
						msg.putHeaders(MessageHeader.MESSAGE_ID, "3mqr0g7j4seej");
						msg.putProperties("PRO_OFFSET", "PRODUCER6_39855");
						msg.putProperties("jjglulc", "yyv090r");
						// logger.info(Thread.currentThread().getName() + " send
						// msg bucket: " + bucket
						// + " ,total msgs num " + localTotalNum);
						p.send(msg);
					}
					for (int i = 0; i < numBuckets; i++) {
						numSendMsgs[i].addAndGet(seqs[i]);
					}
					p.flush();
					sendDoneSignal.countDown();
				}
			});
		}

		System.out.println("Start produce");
		long start = System.currentTimeMillis();
		for (int i = 0; i < numProducers; i++) {
			output_threads[i].start();
		}

		try {
			if (!sendDoneSignal.await(5 * 60, TimeUnit.SECONDS)) {
				for (int i = 0; i < numProducers; i++) {
					output_threads[i].interrupt();
				}
			}
			for (int i = 0; i < numProducers; i++) {
				output_threads[i].join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		long T1 = end - start;
		System.out.println(String.format("Produce cost:%d ms, send:%d q, tps:%d", T1, totalNumSendMsgs.get(),
				totalNumSendMsgs.get() / T1 * 1000));

		// StringBuffer sb = new StringBuffer(400);
		// int total = 0;
		// for (int i = 0; i < numBuckets; i++) {
		// int bucketCap = numSendMsgs[i].get();
		// sb.append(String.format("%4d", bucketCap) + " ");
		// total += bucketCap;
		// }
		// System.out.println(sb.toString());
		// System.out.println("total = " + total + " totalNumSendMsgs = " +
		// totalNumSendMsgs.get());
	}

	/**
	 * compose body of AVERAGE_MSG_SIZE
	 * 
	 * @param producerId
	 * @param bucketId
	 * @param seq
	 * @return
	 */
	static final int AVERAGE_MSG_SIZE = 11;
	static Random rand = new Random();
	static byte[] suffix = new byte[AVERAGE_MSG_SIZE - 6];

	public static byte[] pack(int producerId, int bucketId, int seq) {
		byte[] body = new byte[AVERAGE_MSG_SIZE];
		body[0] = (byte) producerId;
		body[1] = (byte) bucketId;
		for (int i = 2; i < 6; i++) {
			body[i] = (byte) seq;
			seq >>>= 8;
		}
		rand.nextBytes(suffix);
		System.arraycopy(suffix, 0, body, 6, AVERAGE_MSG_SIZE - 6);
		return body;
	}

	/**
	 * get seq of the message
	 * @param body
	 * @return
	 */
	public static int unpack(byte[] body) {
		int seq = 0;
		for (int i = 5; i > 1; i--) {
			seq <<= 8;
			seq |= (body[i] & 0xff);
		}
		return seq;
	}
}
