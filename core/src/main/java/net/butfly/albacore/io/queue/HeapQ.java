package net.butfly.albacore.io.queue;

import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

public class HeapQ<I, O> extends QImpl<I, O> implements Q<I, O> {
	private static final long serialVersionUID = -1;
	private static final Logger logger = Logger.getLogger(HeapQ.class);
	protected final LinkedBlockingQueue<O> impl;
	private Converter<I, O> conv;

	public HeapQ(String name, long capacity, Converter<I, O> conv) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
		this.conv = conv;
	}

	@Override
	public final boolean enqueue(I e) {
		if (null == e) return false;
		try {
			impl.put(conv.apply(e));
			return true;
		} catch (InterruptedException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	public O dequeue() {
		try {
			return impl.take();
		} catch (InterruptedException e) {
			logger.error("Dequeue failure", e);
			return null;
		}
	}

	@Override
	public long size() {
		return impl.size();
	}
}
