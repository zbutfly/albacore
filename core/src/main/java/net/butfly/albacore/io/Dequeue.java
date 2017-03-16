package net.butfly.albacore.io;

import java.util.function.Function;
import java.util.stream.Stream;

@FunctionalInterface
interface Dequeue<V> {
	long dequeue(Function<Stream<V>, Long> using, long batchSize);
}
