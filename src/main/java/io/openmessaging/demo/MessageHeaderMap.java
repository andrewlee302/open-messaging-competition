package io.openmessaging.demo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.openmessaging.MessageHeader;

public class MessageHeaderMap {

	public static HashMap<Byte, String> hashCodeToHeader = new HashMap<>();
	static {
		putHeader(MessageHeader.MESSAGE_ID);
		putHeader(MessageHeader.TOPIC);
		putHeader(MessageHeader.QUEUE);
		putHeader(MessageHeader.BORN_TIMESTAMP);
		putHeader(MessageHeader.BORN_HOST);
		putHeader(MessageHeader.STORE_TIMESTAMP);
		putHeader(MessageHeader.STORE_HOST);
		putHeader(MessageHeader.START_TIME);
		putHeader(MessageHeader.STOP_TIME);
		putHeader(MessageHeader.TIMEOUT);
		putHeader(MessageHeader.PRIORITY);
		putHeader(MessageHeader.RELIABILITY);
		putHeader(MessageHeader.SEARCH_KEY);
		putHeader(MessageHeader.SCHEDULE_EXPRESSION);
		putHeader(MessageHeader.SHARDING_KEY);
		putHeader(MessageHeader.SHARDING_PARTITION);
		putHeader(MessageHeader.TRACE_ID);
	}

	/**
	 * every header has different (byte) hashcode
	 * 
	 * @param header
	 * @return
	 */
	public static byte getIdOfHeader(String header) {
		return (byte) header.hashCode();
	}

	/**
	 * get header strign from header id
	 * 
	 * @param b
	 * @return
	 */
	public static String getHeaderFromId(byte b) {
		return hashCodeToHeader.get(b);
	}

	private static void putHeader(String header) {
		hashCodeToHeader.put(getIdOfHeader(header), header);
	}

	public static Set<String> getKeySet(Map<Byte, Object> kvs) {
	Set<String> keySet = new HashSet<>();
	for (Byte key : kvs.keySet()) {
		keySet.add(MessageHeaderMap.getHeaderFromId(key));
	}
	return keySet;
	}
}
