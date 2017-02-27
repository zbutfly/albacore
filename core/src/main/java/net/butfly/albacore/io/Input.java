package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.io.queue.Queue;

public interface Input<V> extends IO, Dequeue<V> {
	static Input<?> NULL = (using, batchSize) -> {};

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	default <V1> Input<V1> then(Function<V, V1> conv) {
		return Wrapper.wrap(this, (using, batchSize) -> dequeue(s -> using.accept(s.map(conv)), batchSize));
	}

	default <V1> Input<V1> thens(Function<Iterable<V>, Iterable<V1>> conv, int parallelism) {
		return Wrapper.wrap(this, (using, batchSize) -> dequeue(s -> eachs(IO.list(Streams.spatialMap(s, parallelism, t -> conv.apply(
				() -> Its.it(t)).spliterator())), s1 -> using.accept(s1)), batchSize));
	}

	// more extends
	default Input<V> prefetch(Queue<V, V> pool, long fetchSize) {
		return new Input<V>() {
			private final OpenableThread fetch = new OpenableThread(() -> {
				while (opened())
					Input.this.dequeue(s -> pool.enqueue(s), fetchSize);
			}, Input.this.name() + "PrefetcherThread");

			@Override
			public void dequeue(Consumer<Stream<V>> using, long batchSize) {
				pool.dequeue(using, batchSize);
			}

			@Override
			public void close() {
				Input.super.close();
				fetch.close();
			}
		};
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
			using.accept(b.build());
		};
	}

	public static <T> Input<T> of(Iterable<? extends T> collection) {
		return of(collection.iterator());
	}

	public static <T> Input<T> of(Supplier<? extends T> next, Supplier<Boolean> ending) {
		return of(Its.it(next, ending));
	}
}
