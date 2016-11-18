package net.butfly.albacore.io;

import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

public class HeapQueue<I, O> extends QueueImpl<I, O> implements Queue<I, O> {
	private static final long serialVersionUID = -1;
	private static final Logger logger = Logger.getLogger(HeapQueue.class);
	private final LinkedBlockingQueue<O> impl;
	private Converter<I, O> conv;

	public HeapQueue(String name, long capacity, Converter<I, O> conv) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
		this.conv = conv;
	}

	@Override
	protected final boolean enqueueRaw(I e) {
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
	protected O dequeueRaw() {
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
