package net.butfly.albacore.io;

import net.butfly.albacore.base.Namedly;

public abstract class OutputImpl<V> extends Namedly implements Output<V> {
	protected OutputImpl() {
		super();
	}

	protected OutputImpl(String name) {
		super(name);
	}
}
