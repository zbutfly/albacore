package net.butfly.albacore.io;

import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Converter;

public interface Output<V> extends IO, Consumer<Stream<V>> {
	static <V> Output<V> NULL() {
		return new OutputImpl<V>() {
			@Override
			protected boolean enqueue(V item) {
				return true;
			}
		};
	}

	@Override
	default long size() {
		return 0;
	}

	long enqueue(Stream<V> items);

	@Override
	default void accept(Stream<V> items) {
		enqueue(items);
	}

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		Output<V0> o = items -> enqueue(Streams.of(items.map(conv)));
		o.open();
		return o;
	}

	default <V0> Output<V0> priors(Converter<Iterable<V0>, Iterable<V>> conv, int parallelism) {
		Output<V0> o = items -> eachs(Streams.spatial(items, parallelism).values(), s0 -> enqueue(Streams.of(conv.apply(
				(Iterable<V0>) () -> Its.it(s0)))), Streams.LONG_SUM);
		o.open();
		return o;
	}
}
