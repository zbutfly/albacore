package net.butfly.albacore.io.queue;

public final class SimpleQ<V> extends HeapQ<V, V> implements Q<V, V> {
	private static final long serialVersionUID = -1;

	public SimpleQ(String name, long capacity) {
		super(name, capacity, e -> e);
	}
}
