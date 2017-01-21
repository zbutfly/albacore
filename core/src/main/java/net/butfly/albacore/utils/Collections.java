package net.butfly.albacore.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

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

	public static <T> List<T> cleanNull(Collection<T> original) {
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

	public static <T> List<T> asList(Iterable<T> it) {
		if (null == it) return null;
		return it instanceof List ? (List<T>) it : asList(it.iterator());
	}

	public static <T> List<T> asList(Iterator<T> iter) {
		List<T> l = new ArrayList<>();
		while (iter.hasNext())
			l.add(iter.next());
		return l;
	}

	public static <T> List<List<T>> chopped(List<T> origin, int chopSize) {
		List<List<T>> parts = new ArrayList<>();
		int originalSize = origin.size();
		for (int i = 0; i < originalSize; i += chopSize)
			parts.add(new ArrayList<>(origin.subList(i, Math.min(originalSize, i + chopSize))));
		return parts;
	}

}
