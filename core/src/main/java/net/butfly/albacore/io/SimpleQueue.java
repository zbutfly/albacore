package net.butfly.albacore.io;

public final class SimpleQueue<V> extends HeapQueue<V, V> implements Queue<V, V> {
	private static final long serialVersionUID = -1;

	public SimpleQueue(String name, long capacity) {
		super(name, capacity, e -> e);
	}
}
