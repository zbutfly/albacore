package net.butfly.albacore.io;

import java.util.List;
import java.util.stream.Stream;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;

public interface Input<V> extends Openable, Sizable {
	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	V dequeue();

	default Stream<V> dequeue(long batchSize) {
		return IOStreaming.batch(() -> dequeue(), batchSize).parallelStream().filter(t -> t != null);
	}

	default <V1> Input<V1> then(Converter<V, V1> conv) {
		return new InputThenHandler<>(this, conv).proxy(Input.class);
	}

	default <V1> Input<V1> thens(Converter<List<V>, List<V1>> conv) {
		return new InputThensHandler<>(this, conv).proxy(Input.class);
	}
}
