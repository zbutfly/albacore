package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
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
	private final static int c = Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.SIZED
			| Spliterator.SUBSIZED;

	public static <V> Stream<V> batch(long batchSize, Iterator<V> it) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, c), true).limit(batchSize);
	}

	public static <V> Stream<V> batch(long batchSize, Supplier<V> get, Supplier<Boolean> ending) {
		return batch(batchSize, new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return !ending.get();
			}

			@Override
			public V next() {
				return get.get();
			}
		});
	}

	@Deprecated
	public static <V> Stream<V> batchOld(long batchSize, Supplier<V> get, Supplier<Boolean> ending, WriteLock lock,
			boolean retryOnException) {
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

	public static <V> Stream<V> batchSync(long batchSize, Iterator<V> it, WriteLock lock, boolean retryOnException) {
		return batchOld(batchSize, () -> it.next(), () -> it.hasNext(), lock, retryOnException);
	}

	static <V> Stream<List<V>> page(Stream<V> s, long pageSize) {
		return null;
	}

	public static <V> Stream<V> of(Iterator<V> it) {
		return of(new Iterable<V>() {
			@Override
			public Iterator<V> iterator() {
				return it;
			}
		});
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

	public static <V> Stream<V> of(Iterable<V> col, boolean parallel) {
		if (Collection.class.isAssignableFrom(col.getClass())) {
			Stream<V> s = ((Collection<V>) col).stream();
			return parallel ? s.parallel() : s;
		} else return StreamSupport.stream(col.spliterator(), parallel);
	}

	public static <K, V> Stream<Entry<K, V>> of(Map<K, V> map) {
		return of(map.entrySet()).filter(e -> e.getKey() != null && e.getValue() != null);
	}

	@SafeVarargs
	public static <V> Stream<V> of(V... values) {
		return of(Stream.of(values));
	}
}
