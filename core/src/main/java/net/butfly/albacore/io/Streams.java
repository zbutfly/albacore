package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.log4j.Logger;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.async.Concurrents;

public final class Streams extends Utils {
	private static final Logger logger = Logger.getLogger(Streams.class);
	public static final Predicate<Object> NOT_NULL = t -> null != t;

	public static <V> Stream<Iterator<V>> batch(int parallelism, Iterator<V> it) {
		List<Iterator<V>> its = new ArrayList<>();
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		for (int i = 0; i < parallelism; i++)
			its.add(it(it, lock));
		return its.parallelStream();
	}

	public static <V> Iterator<V> it(Supplier<V> get, Supplier<Boolean> end) {
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return !end.get();
			}

			@Override
			public V next() {
				return get.get();
			}
		};
	}

	public static <V> Iterator<V> it(Iterator<V> it, ReentrantReadWriteLock lock) {
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				if (null != lock) lock.writeLock().lock();
				try {
					return it.hasNext();
				} finally {
					if (null != lock) lock.writeLock().unlock();
				}
			}

			@Override
			public V next() {
				if (null != lock) lock.writeLock().lock();
				try {
					if (it.hasNext()) return it.next();
					else return null;
				} finally {
					if (null != lock) lock.writeLock().unlock();
				}
			}
		};
	}

	public static <V> Stream<Iterator<V>> batch(int parallelism, Supplier<V> get, Supplier<Boolean> end) {
		if (parallelism == 1) return Stream.of(it(get, end));
		return batch(parallelism, it(get, end));
	}

	public static <V> Stream<Stream<V>> batch(int parallelism, Stream<V> s) {
		if (parallelism == 1) return Stream.of(s);
		return batch(parallelism, s.iterator()).map(it -> StreamSupport.stream(((Iterable<V>) () -> it).spliterator(),
				DEFAULT_PARALLEL_ENABLE).filter(NOT_NULL));

	}

	public static <V, V1> Stream<Stream<V1>> batchMap(int parallelism, Stream<V> s, Function<Iterable<V>, Iterable<V1>> convs) {
		if (parallelism == 1) return Stream.of(of(convs.apply(() -> s.iterator())));
		Stream<Iterator<V>> b = batch(parallelism, s.iterator());
		return b.parallel().map(it -> {
			return of(convs.apply((Iterable<V>) () -> it));
		});
		// .flatMap(it -> StreamSupport.stream(it .spliterator(),
		// DEFAULT_PARALLEL_ENABLE).filter(NOT_NULL));
	}

	public static <V> Stream<V> fetch(long batchSize, Supplier<V> get, Supplier<Boolean> ending, WriteLock lock, boolean retryOnException) {
		Stream.Builder<V> batch = Stream.builder();
		int c = 0;
		do {
			long prev = c;
			while (!ending.get() && c < batchSize)
				if (null == lock || lock.tryLock()) try {
					V e = get.get();
					if (null != e) {
						batch.add(e);
						c++;
					}
				} catch (Exception ex) {
					logger.warn("Dequeue fail", ex);
					if (!retryOnException) return batch.build();
				} finally {
					if (null != lock) lock.unlock();
				}
			if (c >= batchSize || ending.get()) return batch.build();
			if (c > 0 && c == prev) return batch.build();
			Concurrents.waitSleep();
		} while (true);
	}

	private static final boolean DEFAULT_PARALLEL_ENABLE = Boolean.parseBoolean(Configs.MAIN_CONF.getOrDefault(
			"albacore.parallel.stream.enable", "false"));

	public static <V> Stream<V> of(Stream<V> s) {
		if (DEFAULT_PARALLEL_ENABLE) s = s.parallel();
		return s.filter(NOT_NULL);
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

}
