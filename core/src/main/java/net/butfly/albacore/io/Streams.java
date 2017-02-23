package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

	static <V> List<V> batch(Supplier<V> get, long batchSize) {
		List<V> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			try {
				V e = get.get();
				if (null != e) batch.add(e);
			} catch (Exception ex) {
				logger.warn("Dequeue fail but continue", ex);
			}
			if (batch.size() == 0) Concurrents.waitSleep();
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	static <V> Stream<List<V>> page(Stream<V> get, long pageSize) {
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
		return of(col, true);
	}

	public static <V> Stream<V> of(Iterable<V> col, boolean parallel) {
		return of(Collection.class.isAssignableFrom(col.getClass()) ? (//
		parallel ? of((Collection<V>) col) : ((Collection<V>) col).stream()//
		) : StreamSupport.stream(col.spliterator(), parallel));
	}

	public static <K, V> Stream<Entry<K, V>> of(Map<K, V> map) {
		return Streams.of(map.entrySet()).filter(e -> e.getKey() != null && e.getValue() != null);
	}

	@SafeVarargs
	public static <V> Stream<V> of(V... values) {
		return of(Stream.of(values));
	}
}
