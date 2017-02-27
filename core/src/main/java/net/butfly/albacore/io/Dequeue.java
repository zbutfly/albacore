package net.butfly.albacore.io;

import java.util.function.Consumer;
import java.util.stream.Stream;

@FunctionalInterface
interface Dequeue<V> {
	void dequeue(Consumer<Stream<V>> using, long batchSize);
}
