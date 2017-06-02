package io.openmessaging.demo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.openmessaging.KeyValue;

public class DefaultKeyValue implements KeyValue, Serializable {

	private static final long serialVersionUID = 5163720175219931433L;

	private final Map<String, Object> kvs = new HashMap<>();

	@Override
	public KeyValue put(String key, int value) {
		kvs.put(key, value);
		return this;
	}

	@Override
	public KeyValue put(String key, long value) {
		kvs.put(key, value);
		return this;
	}

	@Override
	public KeyValue put(String key, double value) {
		kvs.put(key, value);
		return this;
	}

	@Override
	public KeyValue put(String key, String value) {
		kvs.put(key, value);
		return this;
	}

	@Override
	public int getInt(String key) {
		return (Integer) kvs.getOrDefault(key, 0);
	}

	@Override
	public long getLong(String key) {
		return (Long) kvs.getOrDefault(key, 0L);
	}

	@Override
	public double getDouble(String key) {
		return (Double) kvs.getOrDefault(key, 0.0d);
	}

	@Override
	public String getString(String key) {
		return (String) kvs.getOrDefault(key, null);
	}

	@Override
	public Set<String> keySet() {
		return kvs.keySet();
	}

	@Override
	public boolean containsKey(String key) {
		return kvs.containsKey(key);
	}
}

class DefaultHeaderKeyValue implements KeyValue, Serializable {
	private static final long serialVersionUID = 1277298135881491652L;

	private final Map<Byte, Object> kvs = new HashMap<>();

	@Override
	public KeyValue put(String key, int value) {
		kvs.put(MessageHeaderMap.getIdOfHeader(key), value);
		return null;
	}

	@Override
	public KeyValue put(String key, long value) {
		kvs.put(MessageHeaderMap.getIdOfHeader(key), value);
		return null;
	}

	@Override
	public KeyValue put(String key, double value) {
		kvs.put(MessageHeaderMap.getIdOfHeader(key), value);
		return null;
	}

	@Override
	public KeyValue put(String key, String value) {
		kvs.put(MessageHeaderMap.getIdOfHeader(key), value);
		return null;
	}

	@Override
	public int getInt(String key) {
		return (Integer) kvs.getOrDefault(MessageHeaderMap.getIdOfHeader(key), 0);
	}

	@Override
	public long getLong(String key) {
		return (Long) kvs.getOrDefault(MessageHeaderMap.getIdOfHeader(key), 0L);
	}

	@Override
	public double getDouble(String key) {
		return (Double) kvs.getOrDefault(MessageHeaderMap.getIdOfHeader(key), 0.0d);
	}

	@Override
	public String getString(String key) {
		return (String) kvs.getOrDefault(MessageHeaderMap.getIdOfHeader(key), null);
	}

	@Override
	public Set<String> keySet() {
		return MessageHeaderMap.getKeySet(kvs);
	}

	@Override
	public boolean containsKey(String key) {
		return kvs.containsKey(MessageHeaderMap.getIdOfHeader(key));
	}

}