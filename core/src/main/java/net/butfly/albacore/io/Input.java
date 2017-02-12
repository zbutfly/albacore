package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public interface Input<V> extends IO, Supplier<V>, Iterator<V> {
	@Override
	default V get() {
		return dequeue();
	}

	@Override
	default boolean hasNext() {
		return !empty();
	}

	@Override
	default V next() {
		return dequeue();
	}

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	V dequeue();

	default Stream<V> dequeue(long batchSize) {
		return Streams.of(Streams.batch(() -> dequeue(), batchSize));
	}

	default <V1> Input<V1> then(Converter<V, V1> conv) {
		return new InputThenHandler<>(this, conv).proxy(Input.class);
	}

	default <V1> Input<V1> thens(Converter<List<V>, List<V1>> conv) {
		return new InputThensHandler<>(this, conv).proxy(Input.class);
	}

	public static <T> Input<T> of(Collection<? extends T> collection) {
		return of(collection.iterator(), false);
	}

	public static <T> Input<T> of(Iterator<? extends T> iter, boolean infinite) {
		return null;
	}

	public static <T> Input<T> of(Supplier<? extends T> supplier) {
		return of(Collections.iterator(supplier), true);
	}
}
