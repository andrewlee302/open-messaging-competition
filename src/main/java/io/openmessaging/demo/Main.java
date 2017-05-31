package io.openmessaging.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.openmessaging.KeyValue;
import io.openmessaging.Producer;

public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		KeyValue properties = new DefaultKeyValue();
		Producer p = new DefaultProducer(properties);
		byte[] buff = new byte[1 << 20];
		SegmentOutputStream sos = new SegmentOutputStream(buff);
		ObjectOutputStream oos = new ObjectOutputStream(sos);
		for (int i = 0; i < 10; i++) {
			DefaultBytesMessage msg = (DefaultBytesMessage) p.createBytesMessageToQueue("BUCKET",
					StressProducerTester.pack(1, 1, i));
			System.out.println(sos.msgBuffer.position());
			oos.writeObject(msg);
		}
		sos.close();
		oos.flush();
		oos.close();

		ObjectInputStream ois = new ObjectInputStream(new SegmentInputStream(buff));
		for (int i = 0; i < 10; i++) {
			DefaultBytesMessage msg = (DefaultBytesMessage) ois.readObject();
			System.out.println(i);
			if (msg.getBody()[0] == msg.getBody()[0]) {
				System.out.println(true);
			} else {
				System.out.println(false);
			}
		}
		ois.close();
		System.out.println("here");
		return;
	}
}

class SegmentOutputStream extends OutputStream {
	ByteBuffer msgBuffer;

	SegmentOutputStream(byte[] buff) {
		msgBuffer = ByteBuffer.wrap(buff, 0, buff.length);
	}

	@Override
	public void write(int b) throws IOException {
		msgBuffer.put((byte) b);
	}

	@Override
	public void write(byte b[], int off, int len) throws IOException {
		msgBuffer.put(b, off, len);
	}

}

class SegmentInputStream extends InputStream {

	ByteBuffer msgBuffer;

	SegmentInputStream(byte[] buff) {
		msgBuffer = ByteBuffer.wrap(buff, 0, buff.length);
	}

	@Override
	public int read() throws IOException {
		return msgBuffer.get();
	}

	@Override
	public int read(byte dst[], int off, int len) throws IOException {
		msgBuffer.get(dst, off, len);
		return len;
	}
}
