package net.butfly.albacore.utils.collection;

import java.util.HashMap;
import java.util.Map;

public class Maps {
	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> of(Object... kvs) {
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i + 1 < kvs.length; i += 2)
			map.put((K) kvs[i], (V) kvs[i + 1]);
		return map;
	}
}
