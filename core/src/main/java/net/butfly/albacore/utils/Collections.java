package net.butfly.albacore.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import net.butfly.albacore.lambda.Converter;
import scala.Tuple2;

public final class Collections extends Utils {
	public static <T, R> Converter<List<T>, List<R>> convAs(Converter<T, R> single) {
		return ts -> ts.stream().map(single::apply).collect(Collectors.toList());
	}

	public static <T, R> List<R> transform(Collection<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		return original.parallelStream().map(trans).collect(Collectors.toList());
	}

	/**
	 * Transform Without Both I/O Null<br>
	 * Filter null and transform (map) collection and filter null again
	 * 
	 * @param original
	 * @param trans
	 * @return
	 */
	public static <T, R> List<R> transN(Collection<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		return original.parallelStream().filter(t -> t != null).map(trans).filter(t -> t != null).collect(Collectors.toList());
	}

	/**
	 * Transform Without Null<br>
	 * Filter null and transform (map) collection
	 * 
	 * @param original
	 * @param trans
	 * @return
	 */
	public static <T, R> List<R> transWN(Collection<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		return original.parallelStream().filter(t -> t != null).map(trans).collect(Collectors.toList());
	}

	/**
	 * Transform Then Not Null Transform (map) collection and filter null in
	 * result
	 * 
	 * @param original
	 * @param trans
	 * @return
	 */
	public static <T, R> List<R> transNN(Collection<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		return original.parallelStream().map(trans).filter(t -> t != null).collect(Collectors.toList());
	}

	public static <T> List<T> noNull(Collection<T> original) {
		if (original == null) return null;
		return original.parallelStream().filter(t -> t != null).collect(Collectors.toList());
	}

	@SafeVarargs
	public static <T, R> List<R> transform(Converter<T, R> trans, T... original) {
		if (original == null) return null;
		List<R> r = new ArrayList<>(original.length);
		for (T t : original)
			r.add(trans.apply(t));
		return r;
	}

	public static <T, R> List<R> transform(Iterable<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		return transform(original.iterator(), trans);
	}

	public static <T, R> List<R> transform(Iterator<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		List<R> r = new ArrayList<>();
		while (original.hasNext())
			r.add(trans.apply(original.next()));
		return r;
	}

	public static <T, K, V> Map<K, V> transMapping(Collection<T> list, Converter<T, Tuple2<K, V>> mapping) {
		Map<K, V> map = new HashMap<>();
		list.forEach(t -> {
			Tuple2<K, V> e = mapping.apply(t);
			map.put(e._1, e._2);
		});
		return map;
	}

	public static <T> List<T> disorderize(Collection<T> origin) {
		if (null == origin) return null;
		ThreadLocalRandom r = ThreadLocalRandom.current();
		List<T> o = new ArrayList<>(origin);
		List<T> d = new ArrayList<>(origin.size());
		while (o.size() > 0)
			d.add(o.remove(Math.abs(r.nextInt()) % o.size()));
		return d;
	}

	public static <T> T random(Iterable<T> origin, int size) {
		int i = (int) (Math.random() * size);
		Iterator<T> it = origin.iterator();
		T e = null;
		while (it.hasNext() && i >= 0)
			e = it.next();
		return e;
	}

	public static <T> List<T> asList(Collection<T> col) {
		if (null == col) return null;
		return col instanceof List ? (List<T>) col : col.stream().collect(Collectors.toList());
	}

	public static <T> List<T> asList(Iterable<T> it) {
		if (null == it) return null;
		if (it instanceof List) return (List<T>) it;
		return StreamSupport.stream(it.spliterator(), false).parallel().collect(Collectors.toList());
	}

	public static <T> List<List<T>> chopped(Collection<T> origin, int chopSize) {
		List<List<T>> parts = new ArrayList<>();
		int originalSize = origin.size();
		for (int i = 0; i < originalSize; i += chopSize)
			parts.add(new ArrayList<>(asList(origin).subList(i, Math.min(originalSize, i + chopSize))));
		return parts;
	}
}
