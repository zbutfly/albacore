package net.butfly.albacore.utils.collection;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.Pair;

public class Maps {
	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> of(Object... kvs) {
		Map<K, V> map = new ConcurrentHashMap<>();
		for (int i = 0; i + 1 < kvs.length; i += 2)
			map.put((K) kvs[i], (V) kvs[i + 1]);
		return map;
	}

	public static Map<String, String> of(Properties props) {
		return Streams.of(props).collect(Collectors.toConcurrentMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
	}

	public static <K, V> Set<Pair<K, V>> pairs(Map<K, V> map) {
		return Streams.of(map).map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toSet());
	}
}
