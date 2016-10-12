package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;

public abstract class OutputQueueImpl<I, D> extends QueueImpl<I, Void, D> implements OutputQueue<I> {
	private static final long serialVersionUID = -1;

	protected OutputQueueImpl(String name) {
		super(name, Long.MAX_VALUE);
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public final boolean enqueue(I e) {
		return (enqueueRaw(e));
	}

	@Override
	public long enqueue(Iterator<I> iter) {
		long c = 0;
		while (iter.hasNext()) {
			I e = iter.next();
			if (null != e && enqueueRaw(e)) c++;
		}
		return c;
	}

	@Override
	public long enqueue(@SuppressWarnings("unchecked") I... e) {
		long c = 0;
		for (int i = 0; i < e.length; i++)
			if (e[i] != null && enqueueRaw(e[i])) c++;
		return c;
	}

	// invalid methods
	@Override
	@Deprecated
	protected final Void dequeueRaw() {
		return null;
	}

	@Override
	@Deprecated
	public final Void dequeue() {
		return null;
	}

	@Override
	@Deprecated
	public final List<Void> dequeue(long batchSize) {
		return null;
	}
}
