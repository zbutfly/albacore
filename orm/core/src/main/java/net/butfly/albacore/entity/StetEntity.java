package net.butfly.albacore.entity;

import java.io.Serializable;

public abstract class StetEntity<K extends Serializable> extends Entity<K> {
	private static final long serialVersionUID = 1L;
	private boolean deleted;

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
}
