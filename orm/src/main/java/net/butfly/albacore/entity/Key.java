package net.butfly.albacore.entity;

import net.butfly.albacore.support.BeanMap;
import net.butfly.albacore.utils.ObjectUtils;

@SuppressWarnings("unchecked")
public abstract class Key<K extends Key<K>> extends Entity<K> {
	private static final long serialVersionUID = 1L;

	public Key() {
		this.id = (K) this;
	}

	@Override
	public boolean equals(Object object) {
		if (object == null || !object.getClass().equals(this.getClass())) return false;
		return ObjectUtils.equals(new BeanMap<Key<K>>(this), new BeanMap<Object>(object));
	}

	public K getId() {
		return (K) this;
	}

	public void setId(K id) {
		this.copy(id);
	}
}
