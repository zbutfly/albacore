package net.butfly.albacore.io.queue;

import java.util.concurrent.atomic.AtomicLong;

public abstract class QImpl<I, O> implements Q<I, O> {
	private static final long serialVersionUID = -1;

	private final String name;
	private final AtomicLong capacity;

	protected QImpl(String name, long capacity) {
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
}
