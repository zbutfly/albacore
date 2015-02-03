package net.butfly.albacore.utils;

import java.util.concurrent.ConcurrentHashMap;

public class Pools extends Utils {
	public static abstract class Pool<K, T> extends ConcurrentHashMap<K, T> {
		private static final long serialVersionUID = -6036093552313665836L;

		protected abstract T create();

		protected T fetch(K key) {
			T existed = this.get(key);
			if (null == existed) {
				existed = this.create();
				this.put(key, existed);
			}
			return existed;
		}
	}
}
