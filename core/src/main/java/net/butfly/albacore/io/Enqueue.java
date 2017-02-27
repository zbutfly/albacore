package net.butfly.albacore.io;

import java.util.stream.Stream;

@FunctionalInterface
interface Enqueue<V> {
	long enqueue(Stream<V> items);
}
