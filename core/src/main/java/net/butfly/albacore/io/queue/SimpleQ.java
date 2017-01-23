package net.butfly.albacore.io.queue;

public final class SimpleQ<V> extends HeapQ<V, V> {
	public SimpleQ(String name, long capacity) {
		super(name, capacity, e -> e);
	}
}
