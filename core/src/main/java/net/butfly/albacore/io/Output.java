package net.butfly.albacore.io;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

public interface Output<V> extends IO, Consumer<V> {
	static <V> Output<V> NULL() {
		return new OutputImpl<V>() {
			@Override
			public boolean enqueue(V item) {
				return true;
			}
		};
	}

	@Override
	default void accept(V t) {
		if (!enqueue(t)) throw new RuntimeException("Enqueue failure");
	}

	@Override
	default long size() {
		return 0;
	}

	boolean enqueue(V item);

	default long enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return 0;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		return c.get();
	}

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		return new OutputPriorHandler<>(this, conv).proxy(Output.class);
	}

	default <V0> Output<V0> priors(Converter<List<V0>, List<V>> conv) {
		return new OutputPriorsHandler<>(this, conv).proxy(Output.class);
	}
}
