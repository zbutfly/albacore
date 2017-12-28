package net.butfly.albacore.utils;

import static net.butfly.albacore.utils.collection.Streams.of;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.utils.collection.Streams;

public final class Collections extends Utils {
	public static <T> BinaryOperator<List<T>> merging() {
		return new BinaryOperator<List<T>>() {
			@Override
			public List<T> apply(List<T> t1, List<T> t2) {
				List<T> l = new ArrayList<T>();
				l.addAll(t1);
				l.addAll(t2);
				return l;
			}
		};
	}

	@Deprecated
	public static <T, R> List<R> map(Collection<T> original, Function<T, R> mapping) {
		if (original == null) return null;
		return of(original).map(mapping).collect(Collectors.<R> toList());
	}

	@Deprecated
	public static <T, R> List<R> transform(Collection<T> original, Function<T, R> trans) {
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
	@Deprecated
	public static <T, R> List<R> mapNoNull(Collection<T> original, Function<T, R> mapping) {
		if (original == null) return null;
		return of(of(original).map(mapping)).collect(Collectors.<R> toList());
	}

	/**
	 * Transform Without Null<br>
	 * Filter null and transform (map) collection
	 * 
	 * @param original
	 * @param mapping
	 * @return
	 */
	@Deprecated
	public static <T, R> List<R> mapNoNullIn(Collection<T> original, Function<T, R> mapping) {
		if (original == null) return null;
		return of(original).map(mapping).collect(Collectors.<R> toList());
	}

	/**
	 * Transform Then Not Null Transform (map) collection and filter null in result
	 * 
	 * @param original
	 * @param mapping
	 * @return
	 */
	@Deprecated
	public static <T, R> List<R> mapNoNullOut(Collection<T> original, Function<T, R> mapping) {
		if (original == null) return null;
		return of(of(original).map(mapping)).collect(Collectors.<R> toList());
	}

	@Deprecated
	public static <T> List<T> noNull(Collection<T> original) {
		if (original == null) return null;
		return of(original).collect(Collectors.<T> toList());
	}

	@SafeVarargs
	@Deprecated
	public static <T, R> List<R> transform(Function<T, R> trans, T... original) {
		if (original == null) return null;
		List<R> r = new ArrayList<R>(original.length);
		for (T t : original)
			r.add(trans.apply(t));
		return r;
	}

	@Deprecated
	public static <T, R> List<R> transform(Iterable<T> original, Function<T, R> trans) {
		if (original == null) return null;
		return transform(original.iterator(), trans);
	}

	@Deprecated
	public static <T, R> List<R> transform(Iterator<T> original, Function<T, R> trans) {
		if (original == null) return null;
		List<R> r = new ArrayList<R>();
		while (original.hasNext())
			r.add(trans.apply(original.next()));
		return r;
	}

	@Deprecated
	public static <T, K, V> Map<K, List<V>> mapMap(Collection<T> list, Function<T, Pair<K, V>> mapping) {
		return of(list).map(mapping).collect(Collectors.groupingByConcurrent(new Function<Pair<K, V>, K>() {
			@Override
			public K apply(Pair<K, V> t) {
				return t.v1();
			}
		}, Collectors.mapping(new Function<Pair<K, V>, V>() {
			@Override
			public V apply(Pair<K, V> t) {
				return t.v2();
			}
		}, Collectors.<V> toList())));
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
		return col instanceof List ? (List<T>) col : of(col).collect(Collectors.<T> toList());
	}

	public static <T> List<T> asList(Iterable<T> it) {
		if (null == it) return null;
		if (it instanceof List) return (List<T>) it;
		return Streams.of(it).collect(Collectors.<T> toList());
	}

	public static <T> Set<T> intersection(Collection<T> c1, final Collection<T> c2) {
		return of(c1).filter(new Predicate<T>() {
			@Override
			public boolean test(T c) {
				return c2.contains(c);
			}
		}).collect(Collectors.<T> toSet());
	}

	private static final Random r = new Random();

	@SuppressWarnings("unchecked")
	private static <T> Collection<List<T>> chop(Stream<T> origin, final int parallelism) {
		if (parallelism <= 1) return Arrays.asList(origin.collect(Collectors.<T> toList()));
		else return origin.collect(Collectors.groupingBy(new Function<T, Integer>() {
			@Override
			public Integer apply(T x) {
				return r.nextInt(parallelism);
			}
		})).values();
	}

	public static <T> Stream<List<T>> chopped(Stream<T> origin, int parallelism) {
		return of(chop(origin, parallelism));
	}

	public static <T> Collection<List<T>> chopped(Collection<T> origin, int blockSize) {
		return chop(of(origin), origin.size() / blockSize);
	}

	@Deprecated
	public static <T> List<List<T>> chopStatic(List<T> origin, int blockSize) {
		List<List<T>> parts = new ArrayList<List<T>>();
		int originalSize = origin.size();
		for (int i = 0; i < originalSize; i += blockSize)
			parts.add(new ArrayList<T>(origin.subList(i, Math.min(originalSize, i + blockSize))));
		return parts;
	}

	public static <T> Iterator<T> iterator(final Supplier<T> supplier) {
		return new Iterator<T>() {
			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public T next() {
				return supplier.get();
			}
		};
	}
}
