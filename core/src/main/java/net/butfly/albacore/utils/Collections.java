package net.butfly.albacore.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.lambda.Converter;
import scala.Tuple2;

public final class Collections extends Utils {

	public static <T, R> List<R> transform(Collection<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		List<R> r = new ArrayList<>(original.size());
		original.forEach(o -> r.add(trans.apply(o)));
		return r;
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
}
