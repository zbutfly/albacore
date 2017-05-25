package net.butfly.albacore.io;

import static net.butfly.albacore.io.utils.Streams.list;
import static net.butfly.albacore.io.utils.Streams.spatialMap;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.io.queue.Queue0;
import net.butfly.albacore.io.utils.Its;

public interface Input<V> extends IO, Dequeue<V>, Supplier<V>, Iterator<V> {
	static Input<?> NULL = (using, batchSize) -> 0;

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	@Override
	default long capacity() {
		return 0;
	}

	default <V1> Input<V1> then(Function<V, V1> conv) {
		return Wrapper.wrap(this, (using, batchSize) -> dequeue(s -> using.apply(s.map(conv)), batchSize));
	}

	default <V1> Input<V1> thens(Function<Iterable<V>, Iterable<V1>> conv) {
		return thens(conv, 1);
	}

	default <V1> Input<V1> thens(Function<Iterable<V>, Iterable<V1>> conv, int parallelism) {
		return Wrapper.wrap(this, (using, batchSize) -> dequeue(//
				s -> eachs(list(spatialMap(s, parallelism, t -> conv.apply(() -> Its.it(t)).spliterator())), s1 -> using.apply(s1)),
				batchSize));
	}

	// more extends
	default Input<V> prefetch(Queue0<V, V> pool, long fetchSize) {
		return Wrapper.wrap(this, new Dequeue<V>() {
			@Override
			public long dequeue(Function<Stream<V>, Long> using, long batchSize) {
				return pool.dequeue(using, batchSize);
			}
		});
	}

	// constructor
	public static <T> Input<T> of(Iterator<? extends T> it) {
		return (using, batchSize) -> {
			Stream.Builder<T> b = Stream.builder();
			AtomicLong count = new AtomicLong();
			T t = null;
			while (it.hasNext()) {
				if ((t = it.next()) != null) b.add(t);
				if (count.incrementAndGet() > batchSize) break;
			}
			using.apply(b.build());
			return count.get();
		};
	}

	public static <T> Input<T> of(Iterable<? extends T> collection) {
		return of(collection.iterator());
	}

	public static <T> Input<T> of(Supplier<? extends T> next, Supplier<Boolean> ending) {
		return of(Its.it(next, ending));
	}

	@Override
	default V get() {
		AtomicReference<V> ref = new AtomicReference<>();
		dequeue(s -> {
			ref.lazySet(s.findFirst().orElse(null));
			return 1L;
		}, 1);
		return ref.get();
	}

	@Override
	default boolean hasNext() {
		return !empty();
	}

	@Override
	default V next() {
		return get();
	}
}
