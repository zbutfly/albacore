package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public abstract class AbstractQueue<E> implements Queue<E>, Statistical<E> {
	private static final long serialVersionUID = 7082069605335516902L;
	private static final Logger logger = Logger.getLogger(AbstractQueue.class);
	static final long INFINITE_SIZE = -1;
	static final long FULL_WAIT_MS = 500;
	static final long EMPTY_WAIT_MS = 500;

	private AtomicLong capacity = new AtomicLong(INFINITE_SIZE);
	private AtomicBoolean orderlyRead = new AtomicBoolean(false);
	private AtomicBoolean orderlyWrite = new AtomicBoolean(false);

	final Statistic stats;
	final String name;

	protected AbstractQueue(String name, long capacity) {
		this.name = name;
		this.capacity = new AtomicLong(capacity);
		stats = statsSize(null) == Statistical.SIZE_NOT_DEFINED ? null
				: new Statistic(Logger.getLogger("Queue.Statistic." + this.getClass().getSimpleName()));
	}

	abstract protected boolean enqueueRaw(E e);

	abstract protected E dequeueRaw();

	@Override
	public final String name() {
		return name;
	}

	@Override
	public long capacity() {
		return capacity.get();
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

	// Dont override

	@Override
	public final boolean empty() {
		return Queue.super.empty();
	}

	@Override
	public final boolean full() {
		return Queue.super.full();
	}

	@Override
	public final boolean enqueue(E e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return (enqueueRaw(e) && null != stats(Act.INPUT, e, () -> size()));

	}

	@Override
	public final E dequeue() {
		while (true) {
			E e = stats(Act.OUTPUT, dequeueRaw(), () -> size());
			if (null != e) return e;
			else if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	@Override
	public List<E> dequeue(long batchSize) {
		List<E> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			E e = dequeueRaw();
			if (null != e) {
				batch.add(stats(Act.OUTPUT, e, () -> size()));
				if (empty()) gc();
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		if (empty()) gc();
		return batch;
	}

	@Override
	public final Statistic stats() {
		return stats;
	}
}
