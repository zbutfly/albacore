package net.butfly.albacore.io.queue;

public final class SimpleQueue<V> extends HeapQueue<V, V> {
	public SimpleQueue(String name, long capacity) {
		super(name, capacity, e -> e);
	}
}
