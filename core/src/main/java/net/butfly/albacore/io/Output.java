package net.butfly.albacore.io;

import java.util.List;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Converter;

public interface Output<V> extends IO {
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

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		return new OutputPriorHandler<>(this, conv).proxy(Output.class);
	}

	@SuppressWarnings("resource")
	default <V0> Output<V0> priors(Converter<List<V0>, List<V>> conv, int batchSize) {
		return new OutputPriorsHandler<>(this, conv, batchSize).proxy(Output.class);
	}
}
