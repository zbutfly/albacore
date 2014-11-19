package net.butfly.albacore.utils;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

public class ResourceCache<K, V> {
	private Map<K, V> cache;

	public ResourceCache() {
		this.cache = Collections.synchronizedMap(new WeakHashMap<K, V>());
	}

	public V get(K key) {
		return this.cache.get(key);
	}

	public V put(K key, V value) {
		return this.cache.put(key, value);
	}

	public V remove(K key) {
		return this.cache.remove(key);
	}
}
