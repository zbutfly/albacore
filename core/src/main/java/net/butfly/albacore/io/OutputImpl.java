package net.butfly.albacore.io;

import java.util.List;

import net.butfly.albacore.io.queue.QImpl;

public abstract class OutputImpl<I> extends QImpl<I, Void> implements Output<I> {
	private static final long serialVersionUID = -1;

	protected OutputImpl(String name) {
		super(name, 0);
	}

	/* disable dequeue on output */
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
