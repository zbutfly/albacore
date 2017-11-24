package net.butfly.albacore.support;

import net.butfly.albacore.utils.imports.utils.meta.MetaUtils;

public abstract class Bean<T extends Beans<T>> implements Beans<T> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public T clone() {
		return (T) MetaUtils.clone(this, this.getClass());
	}

	@Override
	public boolean equals(Object object) {
		return MetaUtils.equals(this, object);
	}

	public Beans<T> copy(Beans<?> src) {
		if (null != src) MetaUtils.copy(src, this);
		return this;
	}

	@Override
	public String toString() {
		return MetaUtils.toMap(this).toString();
	}

	public Object shadowClone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return this;
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
