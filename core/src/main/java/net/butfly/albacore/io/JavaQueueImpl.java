package net.butfly.albacore.io;

import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public abstract class JavaQueueImpl<I, O> extends QueueImpl<I, O, I> implements Queue<I, O> {
	private static final long serialVersionUID = -1;
	private static final Logger logger = Logger.getLogger(JavaQueueImpl.class);
	private final LinkedBlockingQueue<I> impl;

	public JavaQueueImpl(String name, long capacity) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
	}

	abstract protected O conv(I e);

	@Override
	protected final boolean enqueueRaw(I e) {
		if (null == e) return false;
		try {
			impl.put(e);
			return null != stats(Act.INPUT, e);
		} catch (InterruptedException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	protected O dequeueRaw() {
		try {
			return conv(stats(Act.OUTPUT, impl.take()));
		} catch (InterruptedException e) {
			logger.error("Dequeue failure", e);
			return null;
		}
	}

	@Override
	public final boolean enqueue(I e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return (enqueueRaw(e));
	}

	@Override
	public long size() {
		return impl.size();
	}
}
