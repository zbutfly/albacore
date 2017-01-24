package net.butfly.albacore.io;

import net.butfly.albacore.base.Namedly;

public abstract class InputImpl<V> extends Namedly implements Input<V> {
	protected InputImpl() {
		super();
	}

	protected InputImpl(String name) {
		super(name);
	}
}
