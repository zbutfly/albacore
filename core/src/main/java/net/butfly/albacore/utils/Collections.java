package net.butfly.albacore.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.butfly.albacore.lambda.Converter;
import scala.Tuple2;

public final class Collections extends Utils {
	public static <T> BinaryOperator<List<T>> merging() {
		return (t1, t2) -> {
			List<T> l = new ArrayList<>();
			l.addAll(t1);
			l.addAll(t2);
			return l;
		};
	}

	public static <T, R> List<R> stream(Collection<T> original, Converter<Stream<T>, Stream<R>> streaming) {
		if (original == null) return null;
		return streaming.apply(original.parallelStream()).collect(Collectors.toList());
	}

	public static <T, R> List<R> map(Collection<T> original, Converter<T, R> mapping) {
		if (original == null) return null;
		return original.parallelStream().map(mapping).collect(Collectors.toList());
	}

	@Deprecated
	public static <T, R> List<R> transform(Collection<T> original, Converter<T, R> trans) {
		return map(original, trans);
	}

	/**
	 * Transform Without Both I/O Null<br>
	 * Filter null and transform (map) collection and filter null again
	 * 
	 * @param original
	 * @param mapping
	 * @return
	 */
	public static <T, R> List<R> mapNoNull(Collection<T> original, Converter<T, R> mapping) {
		if (original == null) return null;
		return original.parallelStream().filter(t -> t != null).map(mapping).filter(t -> t != null).collect(Collectors.toList());
	}

	/**
	 * Transform Without Null<br>
	 * Filter null and transform (map) collection
	 * 
	 * @param original
	 * @param mapping
	 * @return
	 */
	public static <T, R> List<R> mapNoNullIn(Collection<T> original, Converter<T, R> mapping) {
		if (original == null) return null;
		return original.parallelStream().filter(t -> t != null).map(mapping).collect(Collectors.toList());
	}

	/**
	 * Transform Then Not Null Transform (map) collection and filter null in
	 * result
	 * 
	 * @param original
	 * @param mapping
	 * @return
	 */
	public static <T, R> List<R> mapNoNullOut(Collection<T> original, Converter<T, R> mapping) {
		if (original == null) return null;
		return original.parallelStream().map(mapping).filter(t -> t != null).collect(Collectors.toList());
	}

	public static <T> List<T> noNull(Collection<T> original) {
		if (original == null) return null;
		return original.parallelStream().filter(t -> t != null).collect(Collectors.toList());
	}

	@SafeVarargs
	@Deprecated
	public static <T, R> List<R> transform(Converter<T, R> trans, T... original) {
		if (original == null) return null;
		List<R> r = new ArrayList<>(original.length);
		for (T t : original)
			r.add(trans.apply(t));
		return r;
	}

	@Deprecated
	public static <T, R> List<R> transform(Iterable<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		return transform(original.iterator(), trans);
	}

	@Deprecated
	public static <T, R> List<R> transform(Iterator<T> original, Converter<T, R> trans) {
		if (original == null) return null;
		List<R> r = new ArrayList<>();
		while (original.hasNext())
			r.add(trans.apply(original.next()));
		return r;
	}

	public static <T, K, V> Map<K, List<V>> mapMap(Collection<T> list, Converter<T, Tuple2<K, V>> mapping) {
		return list.parallelStream().map(mapping).collect(Collectors.groupingBy(t -> t._1, Collectors.mapping(t -> t._2, Collectors
				.toList())));
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
		return col instanceof List ? (List<T>) col : col.parallelStream().collect(Collectors.toList());
	}

	public static <T> List<T> asList(Iterable<T> it) {
		if (null == it) return null;
		if (it instanceof List) return (List<T>) it;
		return StreamSupport.stream(it.spliterator(), false).parallel().collect(Collectors.toList());
	}

	public static <T> Set<T> intersection(Collection<T> c1, Collection<T> c2) {
		return c1.parallelStream().filter(t -> null != t && c2.contains(t)).collect(Collectors.toSet());
	}

	private static final Random r = new Random();

	private static <T> Collection<List<T>> chop(Stream<T> origin, int parallenism) {
		return origin.collect(Collectors.groupingBy(x -> r.nextInt(parallenism))).values();
	}

	public static <T> Stream<List<T>> chopped(Stream<T> origin, int parallenism) {
		return chop(origin, parallenism).parallelStream();
	}

	public static <T> Collection<List<T>> chopped(Collection<T> origin, int blockSize) {
		return chop(origin.parallelStream(), origin.size() / blockSize);
	}

	@Deprecated
	public static <T> List<List<T>> chopStatic(List<T> origin, int blockSize) {
		List<List<T>> parts = new ArrayList<>();
		int originalSize = origin.size();
		for (int i = 0; i < originalSize; i += blockSize)
			parts.add(new ArrayList<>(origin.subList(i, Math.min(originalSize, i + blockSize))));
		return parts;
	}
}
