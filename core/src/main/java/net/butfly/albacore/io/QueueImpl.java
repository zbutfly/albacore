package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.async.Concurrents;

public abstract class QueueImpl<I, O, D> implements Queue<I, O>, Statistical<D> {
	private static final long serialVersionUID = -1;

	private final String name;
	private final AtomicLong capacity;

	protected QueueImpl(String name, long capacity) {
		this.name = name;
		this.capacity = new AtomicLong(capacity);
	}

	abstract protected boolean enqueueRaw(I d);

	abstract protected O dequeueRaw();

	@Override
	public final String name() {
		return name;
	}

	@Override
	public final long capacity() {
		return capacity.get();
	}

	@Override
	public boolean empty() {
		return size() == 0;
	}

	@Override
	public final boolean full() {
		return size() >= capacity();
	}

	@Override
	public boolean enqueue(I e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return (enqueueRaw(e));
	}

	@Override
	public long enqueue(Iterable<I> it) {
		return enqueue(it.iterator());
	}

	@Override
	public long enqueue(Iterator<I> iter) {
		long c = 0;
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		while (iter.hasNext()) {
			I e = iter.next();
			if (null != e && enqueueRaw(e)) c++;
		}
		return c;
	}

	@Override
	public long enqueue(@SuppressWarnings("unchecked") I... e) {
		long c = 0;
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		for (int i = 0; i < e.length; i++)
			if (e[i] != null && enqueueRaw(e[i])) c++;
		return c;
	}

	@Override
	public O dequeue() {
		return dequeueWait();
	}

	protected final O dequeueWait() {
		while (true) {
			O e = dequeueRaw();
			if (null == e) return e;
			gc();
			if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	@Override
	public List<O> dequeue(long batchSize) {
		return dequeueWait(batchSize);
	}

	protected final List<O> dequeueWait(long batchSize) {
		List<O> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			O e = dequeueRaw();
			if (null != e) {
				batch.add(e);
				if (empty()) gc();
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		if (empty()) gc();
		return batch;
	}

	protected D stats(Act act, D e) {
		return Statistical.super.stats(act, e, () -> size());
	}

	protected D stats(Act act) {
		return Statistical.super.stats(act, () -> size());
	}
}
