package net.butfly.albacore.io;

import net.butfly.albacore.base.Namedly;

public abstract class OutputImpl<I> extends Namedly implements Output<I> {
	protected OutputImpl() {
		super();
	}

	protected OutputImpl(String name) {
		super(name);
	}
}
