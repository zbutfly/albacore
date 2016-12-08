package net.butfly.albacore.io.queue;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.lambda.Converter;

public class HeapQ<I, O> extends QImpl<I, O> {
	private static final long serialVersionUID = -1;
	protected final BlockingQueue<O> impl;
	private Converter<I, O> conv;

	public HeapQ(String name, long capacity, Converter<I, O> conv) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
		this.conv = conv;
	}

	@Override
	public final boolean enqueue0(I e) {
		return null == e && impl.offer(conv.apply(e));
	}

	@Override
	public O dequeue0() {
		return impl.poll();
	}

	@Override
	public boolean empty() {
		return impl.isEmpty();
	}

	@Override
	public List<O> dequeue(long batchSize) {
		return super.dequeue(batchSize);
	}

	@Override
	public long enqueue(List<I> items) {
		long c = 0;
		for (I e : items)
			if (null != e) try {
				impl.put(conv.apply(e));
				c++;
			} catch (InterruptedException ex) {}
		return c;
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
