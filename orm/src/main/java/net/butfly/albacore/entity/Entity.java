package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.utils.Objects;

public abstract class Entity<K extends Serializable> extends AbstractEntityBase<K> implements AbstractEntity<K> {
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
	public int compareTo(AbstractEntity other) {
		return Objects.compare(this.id, other.getId());
	}
}
