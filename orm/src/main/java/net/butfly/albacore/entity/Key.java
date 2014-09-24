package net.butfly.albacore.entity;

import net.butfly.albacore.support.BeanMap;
import net.butfly.albacore.utils.ObjectUtils;

public abstract class Key<K extends Key<K>> extends AbstractEntity {
	private static final long serialVersionUID = 1L;

	@Override
	public boolean equals(Object object) {
		if (object == null || !object.getClass().equals(this.getClass())) return false;
		return ObjectUtils.equals(new BeanMap<Key<K>>(this), new BeanMap<Object>(object));
	}
}
