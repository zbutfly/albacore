package net.butfly.albacore.support;

import net.butfly.albacore.utils.ObjectUtils;

public abstract class AdvanceObjectSupport<T extends ObjectSupport<T>> extends BaseObjectSupport<T> implements ObjectSupport<T> {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public T clone() {
		return (T) ObjectUtils.clone(this, this.getClass());
	}

	@Override
	public boolean equals(Object object) {
		return ObjectUtils.equals(this, object);
	}

	public ObjectSupport<T> copy(ObjectSupport<?> src) {
		if (null != src) ObjectUtils.copy(src, this);
		return this;
	}

	@Override
	public String toString() {
		return ObjectUtils.toMap(this).toString();
	}
}
