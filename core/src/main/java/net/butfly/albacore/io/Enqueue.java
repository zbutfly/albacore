package net.butfly.albacore.io;

import java.util.stream.Stream;

@FunctionalInterface
public interface Enqueue<V> {
	long enqueue(Stream<V> items) throws EnqueueException;
}
