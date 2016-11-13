package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class InputQueueImpl<O> extends QueueImpl<Void, O, O> implements InputQueue<O> {
	private static final long serialVersionUID = -1;

	protected InputQueueImpl(String name) {
		super(name, Long.MAX_VALUE);
	}

	@Override
	public O dequeue() {
		while (true) {
			O e = dequeueRaw();
			if (null == e) gc();
			return e;
		}
	}

	@Override
	public List<O> dequeue(long batchSize) {
		List<O> batch = new ArrayList<>();
		while (batch.size() < batchSize) {
			O e = dequeueRaw();
			if (null == e) break;
			batch.add(e);
		}
		gc();
		return batch;
	}

	// Invalid methods.

	@Override
	@Deprecated
	protected final boolean enqueueRaw(Void d) {
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
