package net.butfly.albacore.support;

import java.io.Serializable;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.ObjectUtils;

public abstract class CloneSupport<T extends CloneSupport<T>> implements Cloneable, Serializable {
	private static final long serialVersionUID = 2368563230852816358L;
	public final static int METHOD_GET = 0;
	public final static int METHOD_SET = 1;

	public T clone() {
		return this.deepClone();
	}

	/*************************************************************************/

	@Override
	public boolean equals(Object object) {
		return ObjectUtils.equals(this, object);
	}

	@SuppressWarnings("unchecked")
	public T deepClone() {
		BeanMap<T> bm1 = null;
		try {
			bm1 = new BeanMap<T>((T) this.getClass().newInstance());
		} catch (InstantiationException e) {
			throw new SystemException("");
		} catch (IllegalAccessException e) {
			throw new SystemException("");
		}
		BeanMap<T> bm = new BeanMap<T>((T) this);
		for (String prop : bm.keySet()) {
			if (null == bm1.getWriteMethod(prop)) continue;
			Object value = bm.get(prop);
			bm1.put(prop, ObjectUtils.deepClone(value));
		}
		return bm1.getBean();
	}

	public Object shadowClone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return this;
		}
	}

	/*************************************************************************/

	public CloneSupport<T> copy(CloneSupport<?> src) {
		if (null != src) ObjectUtils.copyByProperties(this, src);
		return this;
	}

}
