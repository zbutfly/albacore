package net.butfly.albacore.io.queue;

import java.util.concurrent.atomic.AtomicLong;

public abstract class QueueImpl<I, O> implements Queue<I, O> {
	private final String name;
	private final AtomicLong capacity;

	protected QueueImpl(long capacity) {
		super();
		this.name = Queue.super.name();
		this.capacity = new AtomicLong(capacity);
	}

	protected QueueImpl(String name, long capacity) {
		super();
		this.name = name;
		this.capacity = new AtomicLong(capacity);
	}

	@Override
	public final String name() {
		return name;
	}

	@Override
	public final long capacity() {
		return capacity.get();
	}

	@Override
	public String toString() {
		return name();
	}
}
