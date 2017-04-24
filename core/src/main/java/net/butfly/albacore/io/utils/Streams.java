package net.butfly.albacore.io.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Suppliterator;

public final class Streams extends Utils {
	private static final Logger logger = Logger.getLogger(Streams.class);
	private static final boolean STREAM_DEBUGGING = Boolean.parseBoolean(System.getProperty("albacore.io.stream.debug", "false"));
	public static final Predicate<Object> NOT_NULL = t -> null != t;
	public static final BinaryOperator<Long> LONG_SUM = (r1, r2) -> {
		if (null == r1 && null == r2) return 0L;
		if (null == r1) return r2.longValue();
		if (null == r2) return r1.longValue();
		return r1.longValue() + r2.longValue();
	};

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
		return Streams.of(new Suppliterator<>(get, size, ending));
	}

	private static final boolean DEFAULT_PARALLEL_ENABLE = Boolean.parseBoolean(Configs.gets("albacore.parallel.stream.enable", "false"));

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

	public static <V, A, R> R collect(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return collect(Streams.of(col).map(mapper), collector);
	}

	public static <T, T1, K, V> Map<K, V> map(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
		return collect(col.map(mapper), Collectors.toMap(keying, valuing));
	}

	public static <T, K, V> Map<K, V> map(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
		return collect(col, Collectors.toMap(keying, valuing));
	}

	public static <V, A, R> R mapping(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return collect(mapping.apply(Streams.of(col)), collector);
	}

	public static <V, R> R collect(Iterable<? extends V> col, Collector<? super V, ?, R> collector) {
		return collect(Streams.of(col), collector);
	}

	public static <V, R> R collect(Stream<? extends V> s, Collector<? super V, ?, R> collector) {
		if (STREAM_DEBUGGING && logger.isTraceEnabled()) {
			AtomicLong c = new AtomicLong();
			R r = Streams.of(s).peek(e -> c.incrementAndGet()).collect(collector);
			logger.debug("One stream collected [" + c.get() + "] elements, performance issue?");
			return r;
		} else return Streams.of(s).collect(collector);
	}

	public static <V> List<V> list(Stream<? extends V> stream) {
		return collect(Streams.of(stream), Collectors.toList());
	}

	public static <V, R> List<R> list(Stream<V> stream, Function<V, R> mapper) {
		return collect(Streams.of(stream).map(mapper), Collectors.toList());
	}

	public static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return collect(col, mapper, Collectors.toList());
	}
}
