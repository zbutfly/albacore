package net.butfly.albacore.io.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.lambda.Converter;

public class HeapQueue<I, O> extends QueueImpl<I, O> {
	protected final BlockingQueue<O> impl;
	private Converter<I, O> conv;

	public HeapQueue(String name, long capacity, Converter<I, O> conv) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
		this.conv = conv;
	}

	@Override
	public final boolean enqueue(I e) {
		if (null == e) return false;
		O o = conv.apply(e);
		if (null == o) return false;
		try {
			impl.put(o);
			return true;
		} catch (InterruptedException ex) {
			return false;
		}
	}

	@Override
	public O dequeue() {
		try {
			return impl.take();
		} catch (InterruptedException e) {
			return null;
		}
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
