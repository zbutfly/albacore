package net.butfly.albacore.entity;

import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.ObjectUtils;

public abstract class Key<K extends Key<K>> extends Bean<AbstractEntity<K>> implements AbstractEntity<K> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public K getId() {
		return (K) this;
	}

	@Override
	public void setId(K id) {
		ObjectUtils.copy(id, this);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compareTo(AbstractEntity key) {
		if (null == key) throw new NullPointerException();
		if (!key.getClass().isAssignableFrom(this.getClass()) && !this.getClass().isAssignableFrom(key.getClass())) return -1;
		return ObjectUtils.compare((DualKey) key, this);
	}
}
