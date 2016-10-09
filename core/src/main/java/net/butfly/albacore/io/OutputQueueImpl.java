package net.butfly.albacore.io;

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
