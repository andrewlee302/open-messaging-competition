package io.openmessaging.demo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class WriteBench {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		long start, end;
		int size = 1 << 30;
		
		start = System.currentTimeMillis();
		RandomAccessFile memoryMappedFile = null;
		MappedByteBuffer buffer = null;
		try {
			memoryMappedFile = new RandomAccessFile("fiel1.data", "rw");
			buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < size; i++) {
			buffer.put((byte) 1);
		}
		buffer.force();
		memoryMappedFile.close();
		end = System.currentTimeMillis();
		System.out.println("MappedByteBuffer write: " + (end - start));

		System.gc();
		start = System.currentTimeMillis();
		try {
			memoryMappedFile = new RandomAccessFile("fiel1.data", "r");
			buffer = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < size; i++) {
			buffer.get();
		}
		memoryMappedFile.close();
		end = System.currentTimeMillis();
		System.out.println("MappedByteBuffer read: " + (end - start));

		System.gc();
		start = System.currentTimeMillis();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("file2.data"));
		for (int i = 0; i < size; i++) {
			bos.write(1);
		}
		bos.flush();
		bos.close();
		end = System.currentTimeMillis();
		System.out.println("FileOutputStream write: " + (end - start));

		System.gc();
		start = System.currentTimeMillis();
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream("file2.data"));
		for (int i = 0; i < size; i++) {
			bis.read();
		}
		bis.close();
		end = System.currentTimeMillis();
		System.out.println("FileOutputStream read: " + (end - start));

		System.gc();
		byte[] a = new byte[size];
		byte[] b = new byte[size];
		Random rand = new Random();
		for (int i = 0; i < size; i++)
			a[i] = (byte) rand.nextInt();
		start = System.currentTimeMillis();
		System.arraycopy(a, 0, b, 0, size);
		end = System.currentTimeMillis();
		System.out.println(end - start);
	}

}
