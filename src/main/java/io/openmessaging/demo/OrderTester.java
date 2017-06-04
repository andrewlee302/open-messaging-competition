package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.junit.Assert;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

public class OrderTester {

	static String dirPath = "/Users/andrew/workspace/java/open-messaging-demo/data";
	static HashMap<String, Integer> processedSegmentNum = new HashMap<>();

	static HashMap<Integer, HashMap<Integer, Integer>> producerSeqs = new HashMap<>();

	public static void main(String[] args) {
	
		testOrder();
	}

	public static void testRead() {
		long start, end;
		start = System.currentTimeMillis();
		File dir = new File(dirPath);
		String[] files = dir.list();
		for (int fileId = 0; fileId < files.length - 1; fileId++) {
			String file = fileId + ".data";
			String filename = Paths.get(dirPath, file).toString();
			System.out.println("Read msgs from " + filename);
			RandomAccessFile memoryMappedFile = null;
			long fileSize = 0;
			MappedByteBuffer buffer = null;
			try {
				memoryMappedFile = new RandomAccessFile(filename, "r");
				fileSize = memoryMappedFile.length();
				buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
				memoryMappedFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			buffer.load();
			end = System.currentTimeMillis();
			System.out.println("cost " + (end - start) + " ms");
		}
	}

	public static void testOrder() {
		TreeMap<String, Integer> processedSegNum = new TreeMap<String, Integer>();
		MetaInfo allMetaInfo = InputManager.loadAllMetaInfo(dirPath);
		for (int rank = 0; rank < Config.PARTITION_NUM; rank++) {
			HashMap<Integer, FileSuperSeg> fileSuperSegMap = allMetaInfo.getFileSuperSegMap(rank);
			long start, end;
			start = System.currentTimeMillis();
			File dir = new File(dirPath);
			for (int fileId = 0; fileId < allMetaInfo.getNumDataFiles(rank); fileId++) {
				FileSuperSeg fileSuperSeg = fileSuperSegMap.get(fileId);

				String filename = Config.getFileName(dirPath, rank, fileId);
				System.out.println("Read msgs from " + filename);
				RandomAccessFile memoryMappedFile = null;
				long fileSize = fileSuperSeg.compressedSize;
				MappedByteBuffer buffer = null;
				try {
					memoryMappedFile = new RandomAccessFile(filename, "r");
					buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
					memoryMappedFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				buffer.load();

				DecompressedSuperSegment dss = new DecompressedSuperSegment(buffer, fileSuperSeg.numSegsInSuperSeg,
						fileSuperSeg.compressedSize);
				byte[] superSegmentBinary = new byte[Config.SEGMENT_SIZE * fileSuperSeg.numSegsInSuperSeg];
				dss.decompress(superSegmentBinary);

				int offset = 0;
				int segCursor = 0;
				for (SequentialSegs sss : fileSuperSeg.sequentialSegs) {
					int temp = processedSegNum.getOrDefault(sss.bucket, 0);
					processedSegNum.put(sss.bucket, temp + sss.numSegs);

					// System.out.printf("rank%d, fileId=%d, bucket=%s,
					// offset=%d, numSegs=%d\n", rank, fileId, sss.bucket,
					// segCursor, sss.numSegs);
					for (int i = 0; i < sss.numSegs; i++) {
						ReadableSegment readSegment = ReadableSegment.wrap(superSegmentBinary,
								offset++ * Segment.CAPACITY, Segment.CAPACITY);
						String b = readSegment.bucket;
						if (!sss.bucket.equals(b)) {
							System.out.println("severe--------------------------");
						}
						int cnt = processedSegmentNum.getOrDefault(b, 0);
						processedSegmentNum.put(b, cnt + 1);
						Message msg;
						try {
							while (true) {
								msg = readSegment.read();
								DefaultBytesMessage byteMsg = (DefaultBytesMessage) msg;
								String msgBucket = null;
								String topic = byteMsg.headers().getString(MessageHeader.TOPIC);
								String queue = byteMsg.headers().getString(MessageHeader.QUEUE);
								byte[] body = byteMsg.getBody();
								int producerId = (int) body[0];
								int bucketId = (int) body[1];
								int seq = StressProducerTester.unpack(body);
								if (topic != null) {
									msgBucket = topic;
								} else {
									msgBucket = queue;
								}

								// assert
								Assert.assertEquals(b, msgBucket);
								if (producerSeqs.get(producerId) == null
										|| producerSeqs.get(producerId).get(bucketId) == null) {
									if (seq != 0) {
										error(producerId, bucketId, seq, 0);
									} else {
										correct(producerId, bucketId, seq, 0);
									}
								} else if (seq != producerSeqs.get(producerId).get(bucketId)) {
									error(producerId, bucketId, seq, producerSeqs.get(producerId).get(bucketId));
								} else {
									correct(producerId, bucketId, seq, producerSeqs.get(producerId).get(bucketId));
								}
								HashMap<Integer, Integer> proMap = producerSeqs.getOrDefault(producerId,
										new HashMap<>());
								int tempSeq = proMap.getOrDefault(bucketId, 0);
								proMap.put(bucketId, tempSeq + 1);
								producerSeqs.put(producerId, proMap);
							}
						} catch (SegmentEmptyException e) {
							System.out.println("Read this segs end");
						}
					}
					segCursor += sss.numSegs;
				}
			}
			end = System.currentTimeMillis();
			System.out.println("cost " + (end - start) + " ms");
		}
		for (Map.Entry<String, Integer> entry : processedSegNum.entrySet()) {
			System.out.println("here " + entry.getKey() + ": " + entry.getValue());
		}
	}

	private static void error(int producerId, int bucketId, int seq, int expected) {
		 System.out.println(String.format("E[%d %d %d], %d", producerId,
		 bucketId, seq, expected));
	}

	private static void correct(int producerId, int bucketId, int seq, int expected) {
		// System.out.println(String.format("C[%d %d %d], %d", producerId,
		// bucketId, seq, expected));
	}

}
