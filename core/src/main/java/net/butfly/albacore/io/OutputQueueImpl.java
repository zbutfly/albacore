package net.butfly.albacore.io;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.lambda.Converter;

public abstract class OutputQueueImpl<I, D> extends QueueImpl<I, Void, D> implements OutputQueue<I> {
	private static final long serialVersionUID = -1;
	protected final Converter<I, D> conv;

	protected OutputQueueImpl(String name, Converter<I, D> conv) {
		super(name, Long.MAX_VALUE);
		this.conv = conv;
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
