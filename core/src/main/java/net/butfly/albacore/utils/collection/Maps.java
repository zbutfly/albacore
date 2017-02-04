package net.butfly.albacore.utils.collection;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Maps {
	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> of(Object... kvs) {
		Map<K, V> map = new ConcurrentHashMap<>();
		for (int i = 0; i + 1 < kvs.length; i += 2)
			map.put((K) kvs[i], (V) kvs[i + 1]);
		return map;
	}

	public static Map<String, String> of(Properties props) {
		Map<String, String> map = new ConcurrentHashMap<>();
		props.entrySet().parallelStream().forEach(e -> {
			if (e.getValue() != null) map.put(e.getKey().toString(), e.getValue().toString());
		});
		return map;
	}
}
