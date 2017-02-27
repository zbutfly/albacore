package net.butfly.albacore.io;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.io.queue.Queue0;
import net.butfly.albacore.lambda.Converter;

public interface Output<V> extends IO, Consumer<Stream<V>>, Enqueue<V> {
	static Output<?> NULL = items -> 0;

	@Override
	default long size() {
		return 0;
	}

	@Override
	default void accept(Stream<V> items) {
		enqueue(items);
	}

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		return Wrapper.wrap(this, items -> enqueue(Streams.of(items.map(conv))));
	}

	default <V0> Output<V0> priors(Converter<Iterable<V0>, Iterable<V>> conv, int parallelism) {
		return Wrapper.wrap(this, items -> eachs(Streams.spatial(items, parallelism).values(), s0 -> enqueue(Streams.of(conv.apply(
				(Iterable<V0>) () -> Its.it(s0)))), Streams.LONG_SUM));
	}

	// more extends
	default Output<V> lazy(Queue0<V, V> pool, long lazySize) {
		return null;
	}

	default Output<V> failover(Queue0<V, V> pool, long failoverSize) {
		return null;
	}

	// constructor
	public static <T> Output<T> of(Collection<? super T> underly) {
		return items -> {
			items.forEach(underly::add);
			return underly.size();
		};

	}
}
