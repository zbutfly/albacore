package net.butfly.albacore.utils.collection;

import static net.butfly.albacore.utils.collection.Streams.collect;
import static net.butfly.albacore.utils.collection.Streams.map;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import net.butfly.albacore.utils.Pair;

public class Maps {
	public static <K, V> ConcurrentMap<K, V> of() {
		return new ConcurrentHashMap<>();
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> of(K firstFieldName, Object... firstFieldValueAndOthers) {
		Map<K, V> map = of();
		if (firstFieldValueAndOthers != null && firstFieldValueAndOthers.length > 1) {
			map.put(firstFieldName, (V) firstFieldValueAndOthers[0]);
			for (int i = 1; i + 1 < firstFieldValueAndOthers.length; i += 2)
				map.put((K) firstFieldValueAndOthers[i], (V) firstFieldValueAndOthers[i + 1]);
		}
		return map;
	}

	public static <K, V> Map<K, V> of(K fieldName, V fieldValue) {
		Map<K, V> map = of();
		if (null != fieldName && null != fieldValue) map.put(fieldName, fieldValue);
		return map;
	}

	public static Map<String, String> of(Properties props) {
		return collect(props.entrySet(), Collectors.toConcurrentMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
	}

	public static <K, V> Set<Pair<K, V>> pairs(Map<K, V> map) {
		return map(map.entrySet(), e -> new Pair<>(e.getKey(), e.getValue()), Collectors.toSet());
	}

	@SuppressWarnings("unchecked")
	public static <K, KV> Pair<K, Object[]> parseFirstKey(Object... kvs) {
		if (null == kvs || kvs.length <= 1) return new Pair<>(null, null);
		else return new Pair<>((K) kvs[0], Arrays.copyOfRange(kvs, 1, kvs.length));

	}

}
