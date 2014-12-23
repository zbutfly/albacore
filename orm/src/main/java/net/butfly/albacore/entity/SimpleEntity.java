package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.AdvanceObjectSupport;

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

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object object) {
		if (object == null || this.getId() == null || !object.getClass().equals(this.getClass())) return false;
		if (object instanceof Entity) return this.getId().equals(((Entity<K>) object).getId());
		else return super.equals(object);
	}
}
