package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public interface Input<V> extends IO {
	static <V> Input<V> NULL() {
		return new InputImpl<V>() {
			@Override
			public void dequeue(Consumer<Stream<V>> using, long batchSize) {}
		};
	}

	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	void dequeue(Consumer<Stream<V>> using, long batchSize);

	default <V1> Input<V1> then(Converter<V, V1> conv) {
		Input<V1> i = (using, batchSize) -> dequeue(s -> using.accept(s.map(conv)), batchSize);
		i.open();
		return i;
	}

	default <V1> Input<Stream<V1>> thens(Converter<Iterable<V>, Iterable<V1>> conv, int parallelism) {
		Input<Stream<V1>> i = (using, batchSize) -> dequeue(s -> using.accept(Streams.batchMap(parallelism, s, conv)), batchSize);
		i.open();
		return i;
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
