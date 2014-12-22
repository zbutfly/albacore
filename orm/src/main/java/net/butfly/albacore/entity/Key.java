package net.butfly.albacore.entity;


@SuppressWarnings("unchecked")
public abstract class Key<K extends Key<K>> extends Entity<K> {
	private static final long serialVersionUID = 1L;
	protected transient K id;

	public Key() {
		this.id = (K) this;
	}

	public K getId() {
		return (K) this;
	}

	public void setId(K id) {
		this.id.copy(id);
	}
}
