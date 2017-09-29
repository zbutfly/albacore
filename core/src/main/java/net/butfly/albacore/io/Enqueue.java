package net.butfly.albacore.io;

import java.util.stream.Stream;

@Deprecated
@FunctionalInterface
public interface Enqueue<V> {
	long enqueue(Stream<V> items) throws EnqueueException;
}
