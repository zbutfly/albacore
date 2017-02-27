package net.butfly.albacore.io.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.utils.parallel.Concurrents;

public class JavaQueue<V> extends QueueImpl<V> {
	private BlockingQueue<V> impl;

	public JavaQueue(String name, long capacity) {
		super(name, capacity);
		init(new LinkedBlockingQueue<>((int) capacity));
	}

	public JavaQueue(String name, BlockingQueue<V> impl, long capacity) {
		super(name, capacity);
		init(impl);
	}

	protected void init(BlockingQueue<V> impl) {
		this.impl = impl;
		open();
	}

	@Override
	protected final boolean enqueue(V e) {
		if (null == e) return false;
		do {} while (opened() && !impl.offer(e) && Concurrents.waitSleep());
		return false;
	}

	@Override
	protected V dequeue() {
		if (!opened()) return null;
		V v = null;
		do {} while (opened() && (v = impl.poll()) == null && Concurrents.waitSleep());
		return v;
	}

	@Override
	public boolean empty() {
		return impl.isEmpty();
	}

	@Override
	public boolean full() {
		return impl.remainingCapacity() <= 0;
	}

	@Override
	public long size() {
		return impl.size();
	}
}
