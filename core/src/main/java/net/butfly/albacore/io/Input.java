package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public interface Input<V> extends IO{
	static <V> Input<V> NULL() {
		return new InputImpl<V>() {
			@Override
			protected V dequeue() {
				return null;
			}
		};
	}

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	Stream<V> dequeue(long batchSize);

	default <V1> Input<V1> then(Converter<V, V1> conv) {
		return new InputThenHandler<>(this, conv).proxy(Input.class);
	}

	@SuppressWarnings("resource")
	default <V1> Input<V1> thens(Converter<Iterable<V>, Iterable<V1>> conv, int parallelism) {
		return new InputThensHandler<>(this, conv, parallelism).proxy(Input.class);
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
