package net.butfly.albacore.io;

import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.utils.logger.Logger;

class QueueImpl<E> extends AbstractQueue<E> implements Queue<E> {
	private static final long serialVersionUID = 9187055541366585674L;
	private static final Logger logger = Logger.getLogger(QueueImpl.class);
	private final LinkedBlockingQueue<E> impl;

	public QueueImpl(String name, long capacity) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
	}

	@Override
	protected boolean enqueueRaw(E e) {
		try {
			impl.put(e);
			statsIn(e);
			return true;
		} catch (InterruptedException ex) {
			logger.error("Enqueue failure", ex);
			return false;
		}
	}

	@Override
	protected E dequeueRaw() {
		try {
			return statsOut(impl.take());
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
