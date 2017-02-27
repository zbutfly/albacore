package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Maps;

public final class Streams extends Utils {
	public static final Predicate<Object> NOT_NULL = t -> null != t;
	public static final BinaryOperator<Long> LONG_SUM = (r1, r2) -> {
		if (null == r1 && null == r2) return 0L;
		if (null == r1) return r2;
		if (null == r2) return r1;
		return r1 + r2;
	};

	public static <V> Map<Integer, Spliterator<V>> spatial(Spliterator<V> it, int parallelism) {
		Map<Integer, Spliterator<V>> b = new ConcurrentHashMap<>();
		for (int i = 0; i < parallelism; i++)
			b.put(i, Its.wrap(it));
		return b;
	}

	public static <V> Map<Integer, Spliterator<V>> spatial(Stream<V> s, int parallelism) {
		return parallelism == 1 ? Maps.of(0, s.spliterator()) : spatial(s.spliterator(), parallelism);
	}

	public static <V, V1> Stream<Stream<V1>> spatialMap(Stream<V> s, int parallelism, Function<Spliterator<V>, Spliterator<V1>> convs) {
		return parallelism == 1 ? Stream.of(StreamSupport.stream(convs.apply(s.spliterator()), s.isParallel()))
				: of(spatial(s, parallelism).values()).map(e -> StreamSupport.stream(convs.apply(e), s.isParallel()));
	}

	public static <V> Stream<V> of(Supplier<V> get, long size, Supplier<Boolean> ending) {
		return Streams.of(new Spliterator<V>() {
			private final int characteristics = Spliterator.CONCURRENT | Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.SIZED
					| Spliterator.SUBSIZED;
			private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
			private long est = size;

			@Override
			public synchronized boolean tryAdvance(Consumer<? super V> action) {
				lock.writeLock().lock();
				try {
					boolean next = !ending.get();
					if (!next) est = 0;
					else est--;
					next = next && est > 0;
					if (next) {
						V v = get.get();
						next = v != null;
						if (next) action.accept(v);
					}
					return next;
				} finally {
					lock.writeLock().unlock();
				}
			}

			@Override
			public Spliterator<V> trySplit() {
				return null;
			}

			@Override
			public long estimateSize() {
				lock.readLock().lock();
				try {
					return est;
				} finally {
					lock.readLock().unlock();
				}
			}

			@Override
			public int characteristics() {
				return characteristics;
			}
		});
	}

	private static final boolean DEFAULT_PARALLEL_ENABLE = Boolean.parseBoolean(Configs.MAIN_CONF.getOrDefault(
			"albacore.parallel.stream.enable", "false"));

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

	public static int calcParallelism(long total, long batch) {
		return total == 0 ? 0 : (int) (((total - 1) / batch) + 1);
	}

	public static long calcBatchSize(long total, int parallelism) {
		return total == 0 ? 0 : (((total - 1) / parallelism) + 1);
	}

}
