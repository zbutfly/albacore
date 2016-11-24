package net.butfly.albacore.io;

import java.util.List;

import net.butfly.albacore.io.queue.QImpl;

public abstract class Output<I> extends QImpl<I, Void> {
	private static final long serialVersionUID = -1;

	protected Output(String name) {
		super(name, Long.MAX_VALUE);
	}

	@Override
	public long size() {
		return 0;
	}

	/* disable dequeue on output */
	@Override
	@Deprecated
	public final Void dequeue0() {
		return null;
	}

	@Override
	@Deprecated
	public final List<Void> dequeue(long batchSize) {
		return null;
	}
}
