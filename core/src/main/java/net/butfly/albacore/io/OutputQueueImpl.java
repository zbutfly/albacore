package net.butfly.albacore.io;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public abstract class OutputQueueImpl<I> extends QueueImpl<I, Void, I> implements OutputQueue<I> {
	private static final long serialVersionUID = -1;

	protected OutputQueueImpl(String name) {
		super(name, 0);
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public final boolean enqueue(I e) {
		return enqueueRaw(e);
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

	@SafeVarargs
	@Override
	public final long enqueue(I... e) {
		return enqueue(Arrays.asList(e));
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
