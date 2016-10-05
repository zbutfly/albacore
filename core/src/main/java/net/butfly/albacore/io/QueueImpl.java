package net.butfly.albacore.io;

import java.util.concurrent.LinkedBlockingQueue;

class QueueImpl<E> extends AbstractQueue<E> implements Queue<E> {
	private static final long serialVersionUID = 9187055541366585674L;
	private final LinkedBlockingQueue<E> impl;

	public QueueImpl(String name, long capacity) {
		super(name, capacity);
		impl = new LinkedBlockingQueue<>((int) capacity);
	}

	@Override
	protected boolean enqueueRaw(E e) {
		try {
			impl.put(e);
			return true;
		} catch (InterruptedException ex) {
			return false;
		}
	}

	@Override
	protected E dequeueRaw() {
		try {
			return impl.take();
		} catch (InterruptedException e) {
			return null;
		}
	}

	@Override
	public long size() {
		return impl.size();
	}
}
