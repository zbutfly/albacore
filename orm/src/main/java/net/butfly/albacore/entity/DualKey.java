package net.butfly.albacore.entity;

import java.io.Serializable;

public abstract class DualKey<K1 extends Serializable, K2 extends Serializable> extends Key<DualKey<K1, K2>> {
	private static final long serialVersionUID = -3917780888129566654L;
}
