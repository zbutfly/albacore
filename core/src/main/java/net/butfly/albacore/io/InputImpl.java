package net.butfly.albacore.io;

import net.butfly.albacore.base.Namedly;

public abstract class InputImpl<O> extends Namedly implements Input<O> {
	protected InputImpl() {
		super();
	}

	protected InputImpl(String name) {
		super(name);
	}
}
