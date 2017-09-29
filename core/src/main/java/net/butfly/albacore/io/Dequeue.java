package net.butfly.albacore.io;

import java.util.function.Function;
import java.util.stream.Stream;

@Deprecated
@FunctionalInterface
public interface Dequeue<V> {
	long dequeue(Function<Stream<V>, Long> using, int batchSize);
}
