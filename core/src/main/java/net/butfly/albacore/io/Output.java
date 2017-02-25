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
		// return new OutputPriorHandler<>(this, conv).proxy(Output.class);
	}

	default <V0> Output<V0> priors(Converter<Iterable<V0>, Iterable<V>> conv, int parallelism) {
		Output<V0> o = items -> IO.run(() -> Streams.batch(parallelism, items).mapToLong(s0 -> enqueue(Streams.of(conv.apply(
				(Iterable<V0>) () -> s0.iterator())))).sum());
		o.open();
		return o;
		// return new OutputPriorsHandler<>(this, conv,
		// parallelism).proxy(Output.class);
	}

	default Output<Stream<V>> stream() {
		Output<Stream<V>> o = items -> {
			return IO.run(() -> items.parallel().mapToLong(s -> enqueue(s)).sum());
		};
		o.open();
		return o;
	}
}
