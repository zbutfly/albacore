package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
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

	public static <V> Stream<V> of(Iterator<V> it) {
		return of(new Iterable<V>() {
			@Override
			public Iterator<V> iterator() {
				return it;
			}
		});
	}

	public static <V> Stream<V> of(Stream<V> s) {
		return s.parallel().filter(NOT_NULL);
	}

	public static <V> Stream<V> of(Iterable<V> col) {
		return of(col, true);
	}

	public static <V> Stream<V> of(Iterable<V> col, boolean parallel) {
		return of(Collection.class.isAssignableFrom(col.getClass()) ? (//
		parallel ? ((Collection<V>) col).parallelStream() : ((Collection<V>) col).stream()//
		) : StreamSupport.stream(col.spliterator(), parallel));
	}

	public static <K, V> Stream<Entry<K, V>> of(Map<K, V> map) {
		return Streams.of(map.entrySet()).filter(e -> e.getKey() != null && e.getValue() != null);
	}

	public static void main(String... args) {
		of(of(Arrays.asList("1", "2", null, null, "3").iterator())).map(e -> {
			System.out.println(e);
			return e;
		}).count();
	}
}
