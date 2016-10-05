package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public abstract class AbstractQueue<E> implements Queue<E> {
	private static final long serialVersionUID = 7082069605335516902L;
	private static final Logger logger = Logger.getLogger(AbstractQueue.class);
	static final long INFINITE_SIZE = -1;
	static final long FULL_WAIT_MS = 5000;
	static final long EMPTY_WAIT_MS = 5000;

	private AtomicLong capacity = new AtomicLong(INFINITE_SIZE);
	private AtomicBoolean orderlyRead = new AtomicBoolean(false);
	private AtomicBoolean orderlyWrite = new AtomicBoolean(false);
	private AtomicBoolean running = new AtomicBoolean(false);

	final Statistic stats;
	final String name;

	protected AbstractQueue(String name, long capacity) {
		this.name = name;
		this.capacity = new AtomicLong(capacity);
		stats = statsSize(null) == Long.MIN_VALUE ? new Statistic(Logger.getLogger(this.getClass().getSimpleName() + ".Statistic")) : null;
	}

	@Override
	public final String name() {
		return name;
	}

	@Override
	public long capacity() {
		return capacity.get();
	}

	@Override
	public void run() {
		running.compareAndSet(false, true);
	}

	@Override
	public void close() {
		running.compareAndSet(true, false);
	}

	@Override
	public final boolean running() {
		return running.get();
	}

	@Override
	public final boolean isReadOrderly() {
		return orderlyRead.get();
	}

	@Override
	public final boolean isWriteOrderly() {
		return orderlyWrite.get();
	}

	@Override
	public final void setReadOrderly(boolean orderly) {
		orderlyRead.set(orderly);
	}

	@Override
	public final void setWriteOrderly(boolean orderly) {
		orderlyWrite.set(orderly);
	}

	@Override
	public final void stats(E e, boolean trueInOrFalseOut) {
		if (null != stats) {
			if (trueInOrFalseOut) stats.in(statsSize(e));
			else stats.out(statsSize(e));
		}
	}

	protected long statsSize(E e) {
		return Long.MIN_VALUE;
	}

	// Dont override

	@Override
	public final boolean empty() {
		return Queue.super.empty();
	}

	@Override
	public final long enqueue(Iterator<E> iter) {
		return Queue.super.enqueue(iter);
	}

	@Override
	public final long enqueue(Iterable<E> it) {
		return Queue.super.enqueue(it);
	}

	@Override
	public final List<E> dequeue(long batchSize) {
		return Queue.super.dequeue(batchSize);
	}

	@Override
	public final boolean enqueue(E e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return enqueueRaw(e);
	}

	@Override
	public final E dequeue() {
		while (true) {
			E e = dequeueRaw();
			if (null != e) {
				stats.out(statsSize(e));
				return e;
			} else if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	abstract protected boolean enqueueRaw(E e);

	abstract protected E dequeueRaw();
}
