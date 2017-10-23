package net.butfly.albacore.utils.collection;

import static net.butfly.albacore.utils.parallel.Parals.run;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.parallel.Suppliterator;

public final class Streams extends Utils {
	public static final Predicate<Object> NOT_NULL = t -> null != t;
	@SuppressWarnings("rawtypes")
	public static final Predicate<Map> NOT_EMPTY_MAP = t -> null != t && !t.isEmpty();

	public static final BinaryOperator<Long> LONG_SUM = (r1, r2) -> {
		if (null == r1 && null == r2) return 0L;
		if (null == r1) return r2.longValue();
		if (null == r2) return r1.longValue();
		return r1.longValue() + r2.longValue();
	};

	public static <V> Future<?> batching(Stream<V> s, Consumer<Stream<V>> using, int batchSize) {
		return Its.split(s.spliterator(), batchSize, it -> {
			using.accept(Streams.of(it));
		});
	}

	public static <V> Map<Integer, Spliterator<V>> spatial(Spliterator<V> it, int parallelism) {
		if (parallelism <= 1) return Maps.of(0, it);
		Map<Integer, Spliterator<V>> b = new ConcurrentHashMap<>();
		for (int i = 0; i < parallelism; i++)
			b.put(i, Its.wrap(it));
		return b;
	}

	public static <V> Map<Integer, Spliterator<V>> spatial(Stream<V> s, int parallelism) {
		return spatial(s.spliterator(), parallelism);
	}

	public static <V, V1> Stream<Stream<V1>> spatialMap(Stream<V> s, int parallelism, Function<Spliterator<V>, Spliterator<V1>> convs) {
		// XXX: bug? on 1, StreamSupport.stream(s.spliterator(), s.isParallel())
		// not work?
		return of(spatial(s, parallelism).values()).map(e -> StreamSupport.stream(convs.apply(e), s.isParallel()));
	}

	public static <V> Stream<V> of(Supplier<V> get, long size, Supplier<Boolean> ending) {
		return of(new Suppliterator<>(get, size, ending));
	}

	private static final boolean DEFAULT_PARALLEL_ENABLE = Boolean.parseBoolean(Configs.gets(Albacore.Props.PROP_STREAM_PARALLES, "false"));

	public static <V> Stream<V> of(Stream<V> s) {
		if (DEFAULT_PARALLEL_ENABLE) s = s.parallel();
		// else s = s.sequential();
		return s.filter(NOT_NULL);
	}

	public static <V> Stream<V> of(Spliterator<V> it) {
		return StreamSupport.stream(it, DEFAULT_PARALLEL_ENABLE).filter(NOT_NULL);
	}

	public static <V> Stream<V> of(Iterable<V> col) {
		return of(col, DEFAULT_PARALLEL_ENABLE);
	}

	public static <V> Stream<V> of(Iterator<V> it) {
		return of(() -> it);
	}

	public static <V> Stream<V> of(Iterable<V> col, boolean parallel) {
		if (null == col) return Stream.empty();
		Stream<V> s;
		if (Collection.class.isAssignableFrom(col.getClass())) {
			Collection<V> c = (Collection<V>) col;
			s = parallel ? c.parallelStream() : c.stream();
		} else s = StreamSupport.stream(col.spliterator(), parallel);
		return s.filter(NOT_NULL);
	}

	public static <K, V> Stream<Entry<K, V>> of(Map<K, V> map) {
		return of(map.entrySet()).filter(e -> e.getKey() != null && e.getValue() != null);
	}

	@SafeVarargs
	public static <V> Stream<V> of(V... values) {
		return of(Stream.of(values));
	}

	// map reduce ops

	public static <V, A, R> R collect(Iterable<? extends V> col, Function<? super V, ? extends A> mapper,
			Collector<? super A, ?, R> collector) {
		return collect(of(col, true).map(mapper), collector);
	}

	public static <T, T1, K, V> Map<K, V> toMap(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
		return collect(col.map(mapper), Collectors.toMap(keying, valuing));
	}

	public static <T, K, V> Map<K, V> toMap(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
		return collect(col, Collectors.toMap(keying, valuing));
	}

	public static <V, A, R> R maps(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return collect(mapping.apply(of(col, true)), collector);
	}

	public static <V, A, R> R map(Iterable<? extends V> col, Function<? super V, ? extends A> mapping,
			Collector<? super A, ?, R> collector) {
		return collect(of(col, true).map(mapping), collector);
	}

	public static <V, A, R> R map(Iterable<V> col, Function<V, A> mapping, Predicate<? super A> filtering,
			Collector<? super A, ?, R> collector) {
		return collect(of(col).map(mapping).filter(filtering), collector);
	}

	public static <V, A, R> R map(Stream<V> col, Function<V, A> mapping, Collector<? super A, ?, R> collector) {
		return collect(of(col).map(mapping), collector);
	}

	public static <V, A> Stream<A> map(Stream<V> col, Function<V, A> mapping) {
		return of(col).map(mapping);
	}

	public static <V, A> Stream<A> map(Iterable<V> col, Function<V, A> mapping) {
		return of(col).map(mapping);
	}

	/**
	 * Base thread pool wrapped collect
	 * 
	 * @param s
	 * @param collector
	 * @return
	 */
	public static <V, R> R collect(Stream<? extends V> s, Collector<? super V, ?, R> collector) {
		return run(() -> of(s).collect(collector));
	}

	public static <V, R> R collect(Iterable<? extends V> col, Collector<? super V, ?, R> collector) {
		return collect(of(col), collector);
	}

	public static <V> List<V> list(Stream<? extends V> stream) {
		return collect(of(stream), Collectors.toList());
	}

	public static <V, R> List<R> list(Stream<V> stream, Function<V, R> mapper) {
		return collect(of(stream).map(mapper), Collectors.toList());
	}

	public static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return collect(col, mapper, Collectors.toList());
	}
}
