package net.butfly.albacore.io;

import java.util.Iterator;

public abstract class InputQueueImpl<O, D> extends QueueImpl<Void, O, D> implements Queue<Void, O> {
	private static final long serialVersionUID = -1;

	protected InputQueueImpl(String name, long capacity) {
		super(name, capacity);
	}

	@Override
	@Deprecated
	protected boolean enqueueRaw(Void d) {
		return false;
	}

	@Override
	@Deprecated
	public final boolean enqueue(Void e) {
		return false;
	}

	@Override
	@Deprecated
	public final long enqueue(Iterator<Void> iter) {
		return 0;
	}

	@Override
	@Deprecated
	public final long enqueue(Iterable<Void> it) {
		return 0;
	}

	@Override
	@Deprecated
	public final long enqueue(Void... e) {
		return 0;
	}
}
