package net.butfly.albacore.io;

import java.util.function.Consumer;
import java.util.stream.Stream;

@FunctionalInterface
public interface Dequeuer<V> {
	void dequeue(Consumer<Stream<V>> using, int batchSize);
}
