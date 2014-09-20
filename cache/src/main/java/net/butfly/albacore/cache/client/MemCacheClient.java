package net.butfly.albacore.cache.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface MemCacheClient {
	Object get(String s) throws TimeoutException;

	void set(String s, int i, Object obj);

	void delete(String s);

	boolean deleteWhithResult(String s);

	void replace(String s, int i, Object obj);

	void append(String s, Object obj);

	void prepend(String s, Object obj);

	void casMax(String s, int i, Object obj);

	boolean cas(String s, int i, Object obj, long cas);

	long getCas(String s);

	Map<InetSocketAddress, Map<String, String>> getStats();
}
