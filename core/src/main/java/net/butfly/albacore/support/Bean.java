package net.butfly.albacore.support;

import net.butfly.albacore.utils.ObjectUtils;

public abstract class Bean<T extends Beans<T>> implements Beans<T> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public T clone() {
		return (T) ObjectUtils.clone(this, this.getClass());
	}

	@Override
	public int compareTo(T object) {
		if (null == object) throw new NullPointerException();
		return ObjectUtils.compare(this, object);
	}

	@Override
	public boolean equals(Object object) {
		return ObjectUtils.equals(this, object);
	}

	public Beans<T> copy(Beans<?> src) {
		if (null != src) ObjectUtils.copy(src, this);
		return this;
	}

	@Override
	public String toString() {
		return ObjectUtils.toMap(this).toString();
	}

	public Object shadowClone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return this;
		}
	}
}
