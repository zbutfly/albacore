package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.AdvanceObjectSupport;
import net.butfly.albacore.utils.ObjectUtils;

public abstract class SimpleEntity<K extends Serializable> extends AdvanceObjectSupport<AbstractEntity> implements Entity<K> {
	private static final long serialVersionUID = -1L;
	protected K id;

	@Override
	public K getId() {
		return id;
	}

	@Override
	public void setId(K id) {
		this.id = id;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compareTo(AbstractEntity key) {
		if (null == key) throw new NullPointerException();
		if (!key.getClass().isAssignableFrom(this.getClass()) && !this.getClass().isAssignableFrom(key.getClass())) return -1;
		return ObjectUtils.compare((DualKey) key, this);
	}
}
