package net.butfly.albacore.io;

import java.util.Iterator;

import net.butfly.albacore.io.queue.QImpl;

public abstract class InputImpl<O> extends QImpl<Void, O> implements Input<O> {
	private static final long serialVersionUID = -1;

	protected InputImpl(String name) {
		super(name, -1);
	}

	/* disable enqueue on input */

	@Override
	@Deprecated
	public final boolean enqueue(Void d) {
		return Input.super.enqueue(d);
	}

	@Override
	@Deprecated
	public final long enqueue(Iterator<Void> iter) {
		return Input.super.enqueue(iter);
	}

	@Override
	@Deprecated
	public final long enqueue(Iterable<Void> it) {
		return Input.super.enqueue(it);
	}

	@Override
	@Deprecated
	public final long enqueue(Void... e) {
		return Input.super.enqueue(e);
	}
}
