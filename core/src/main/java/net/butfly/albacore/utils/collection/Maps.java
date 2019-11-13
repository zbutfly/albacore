package net.butfly.albacore.utils.collection;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.logger.Logger;

public class Maps {
	private static final Logger logger = Logger.getLogger(Maps.class);

	public static <K, V> ConcurrentMap<K, V> of() {
		return new ConcurrentHashMap<>();
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> of(K firstFieldName, Object... firstFieldValueAndOthers) {
		Map<K, V> map = of();
		V v;
		if (firstFieldValueAndOthers != null && firstFieldValueAndOthers.length > 0) {
			if (null != firstFieldValueAndOthers[0]) map.put(firstFieldName, (V) firstFieldValueAndOthers[0]);
			for (int i = 1; i + 1 < firstFieldValueAndOthers.length; i += 2) if (null != (v = (V) firstFieldValueAndOthers[i + 1])) map.put(
					(K) firstFieldValueAndOthers[i], v);
		}
		return map;
	}

	public static <K, V> Map<K, V> of(K fieldName, V fieldValue) {
		Map<K, V> map = of();
		if (null != fieldName && null != fieldValue) map.put(fieldName, fieldValue);
		return map;
	}

	public static <K, V> Map<K, BlockingQueue<V>> ofQ(Iterable<V> values, Function<V, K> keying) {
		Map<K, BlockingQueue<V>> map = of();
		values.forEach(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			map.computeIfAbsent(k, kk -> new LinkedBlockingQueue<>()).add(v);
		});
		return map;
	}

	public static <K, V> Map<K, BlockingQueue<V>> ofQ(Sdream<V> values, Function<V, K> keying) {
		Map<K, BlockingQueue<V>> map = of();
		values.eachs(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			map.computeIfAbsent(k, kk -> new LinkedBlockingQueue<>()).add(v);
		});
		return map;
	}

	public static <K, V> Map<K, List<V>> of(Iterable<V> values, Function<V, K> keying) {
		Map<K, List<V>> map = of();
		values.forEach(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			map.computeIfAbsent(k, kk -> Colls.list()).add(v);
		});
		return map;
	}

	public static <K, V, V1> Map<K, List<V1>> of(Iterable<V> values, Function<V, K> keying, Function<V, V1> valuing) {
		Map<K, List<V1>> map = of();
		values.forEach(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			V1 v1 = valuing.apply(v);
			if (null == v1) return;
			map.computeIfAbsent(k, kk -> Colls.list()).add(v1);
		});
		return map;
	}

	public static <K, V> Map<K, V> distinct(Iterable<V> values, Function<V, K> keying) {
		Map<K, V> map = of();
		values.forEach(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			map.putIfAbsent(k, v);
		});
		return map;
	}

	public static <K, V, V1> Map<K, V1> distinct(Iterable<V> values, Function<V, K> keying, Function<V, V1> valuing) {
		Map<K, V1> map = of();
		values.forEach(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			V1 v1 = valuing.apply(v);
			if (null == v1) return;
			map.putIfAbsent(k, v1);
		});
		return map;
	}

	public static <K, V> Map<K, List<V>> of(Sdream<V> values, Function<V, K> keying) {
		Map<K, List<V>> map = of();
		values.eachs(v -> {
			if (null == v) return;
			K k = keying.apply(v);
			if (null == k) return;
			map.computeIfAbsent(k, kk -> Colls.list()).add(v);
		});
		return map;
	}

	public static Map<String, String> of(Properties props) {
		Map<String, String> m = of();
		for (String key : props.stringPropertyNames()) m.put(key, props.getProperty(key));
		return m;
	}

	public static <K, V> Pair<List<K>, List<V>> lists(Map<K, V> map) {
		List<K> keys = Colls.list();
		List<V> values = Colls.list();
		for (Entry<K, V> e : map.entrySet()) if (null != e.getKey() && null != e.getValue()) {
			keys.add(e.getKey());
			values.add(e.getValue());
		}
		return new Pair<>(keys, values);
	}

	public static <K, V> Set<Pair<K, V>> pairs(Map<K, V> map) {
		Set<Pair<K, V>> s = Colls.distinct();
		map.entrySet().forEach(

				e -> s.add(new Pair<>(e.getKey(), e.getValue())));
		return s;

	}

	@SuppressWarnings("unchecked")
	public static <K, KV> Pair<K, Object[]> parseFirstKey(Object... kvs) {
		if (null == kvs || kvs.length <= 1) return new Pair<>(null, null);
		else return new Pair<>((K) kvs[0], Arrays.copyOfRange(kvs, 1, kvs.length));

	}

	private static Pattern indexed = Pattern.compile("^(.*)\\[(\\d+)\\]$");

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> T getFlat(Map<String, Object> map, String... key) {
		Object v = map;
		for (int i = 0; i < key.length; i++) {
			if (!(v instanceof Map)) throw new RuntimeException("embedded entry not found for [" + key[i] + "] in: " + v.toString());
			Map<String, Object> ss = (Map<String, Object>) v;
			Matcher m = indexed.matcher(key[i]);
			if (m.find()) {
				int index = Integer.parseInt(m.group(2));
				Object l = ss.get(m.group(1));
				if (l instanceof List) v = ((List) l).get(index);
				else if (l.getClass().isArray()) v = Array.get(l, index);
				else throw new RuntimeException("indexed entry not found for [" + key[i] + "] in: " + v.toString());
			} else v = ((Map<String, Object>) v).get(key[i]);
		}
		return (T) v;
	}

	public static <T> T getFlat(Map<String, Object> map, String key) {
		return getFlat(map, key.split("\\."));
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> dimensionize(Map<String, Object> map) {
		Map<String, Object> r = Maps.of();
		for (String key : map.keySet()) {
			Object v0 = map.get(key);
			if (null == v0) continue;
			Object v = dimensionize(v0);
			String[] ks = key.split("\\.");
			if (ks.length == 1) {
				Matcher m = indexed.matcher(key);
				if (!m.find()) r.put(key, v);
				else r.compute(m.group(1), (k, orig) -> {
					if (null == orig) orig = Colls.list();
					else if (orig instanceof Iterable && !(orig instanceof List)) orig = Colls.list((Iterable<Object>) orig);
					((List<Object>) orig).set(Integer.parseInt(m.group(2)), v);
					return orig;
				});
			} else {
				Matcher m = indexed.matcher(ks[0]);
				if (!m.find()) r.compute(ks[0], (k, orig) -> {
					if (null == orig) orig = Maps.of();
					else if (!(orig instanceof Map)) orig = Maps.of("", orig);
					((Map<String, Object>) orig).put(key.substring(ks[0].length() + 1), v);
					return orig;
				});
				else r.compute(m.group(1), (k, orig) -> {
					if (null == orig) orig = Colls.list();
					else if (orig instanceof Iterable && !(orig instanceof List)) orig = Colls.list((Iterable<Object>) orig);
					((List<Object>) orig).set(Integer.parseInt(m.group(2)), v);
					return orig;
				});
			}
		}
		return r;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Object dimensionize(Object v) {
		if (v instanceof Map) return dimensionize((Map<String, Object>) v);
		else if (v instanceof Iterable) return dimensionize((Iterable) v);
		else return v;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> dimensionize(Iterable<Object> v) {
		List<Object> l = Colls.list();
		for (Object e : v) {
			if (null == e) continue;
			if (e instanceof Map) l.add(dimensionize((Map<String, Object>) e));
			else if (e instanceof Iterable) l.add(dimensionize((Iterable<Object>) e));
			else if (e.getClass().isArray()) {
				List<Object> sub = Colls.list();
				for (int i = 0; i < Array.getLength(e); i++) {
					Object vv = Array.get(e, i);
					if (null != vv) sub.add(vv);
				}
				l.add(e);
			}
		}
		return l;
	}

	public static Map<String, String> ofQueryString(String qs) {
		return ofQueryString(qs, "");
	}

	public static Map<String, String> ofQueryString(String qs, String defaultKey) {
		Map<String, String> map = Maps.of();
		for (String kv : qs.split("&")) {
			String[] kvs = kv.split("=", 2);
			String k, v;
			try {
				if (kvs.length < 2) {
					k = defaultKey;
					v = URLDecoder.decode(kvs[0], "utf-8");
				} else {
					k = URLDecoder.decode(kvs[0], "utf-8");
					v = URLDecoder.decode(kvs[1], "utf-8");
				}
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			map.compute(k, (key, orig) -> {
				if (orig == null) return v;
				logger.warn("Duplication key [" + (key.isEmpty() ? "EMPTY" : key) + "] in query string, new value [" + v + "], original value ["
						+ orig + "] lost.");
				return v;
			});
		}
		return map;
	}
}
