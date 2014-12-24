package net.butfly.albacore.entity;

import java.io.Serializable;
import java.lang.reflect.Array;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.ObjectUtils;

public abstract class Entity<K extends Serializable> extends Bean<AbstractEntity<K>> implements AbstractEntity<K> {
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
	public int compareTo(AbstractEntity key) {
		if (null == key) throw new NullPointerException();
		if (!key.getClass().isAssignableFrom(this.getClass()) && !this.getClass().isAssignableFrom(key.getClass())) return -1;
		return ObjectUtils.compare((DualKey) key, this);
	}

	public static Class<?> getKeyClass(Class<? extends AbstractEntity<?>> entityClass) {
		try {
			return entityClass.getMethod("getId").getReturnType();
		} catch (SecurityException e) {
			throw new SystemException("", e);
		} catch (Throwable e) {
			throw new SystemException("", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <K extends Serializable> K[] getKeyBuffer(Class<? extends AbstractEntity<K>> entityClass, int length) {
		return (K[]) Array.newInstance(getKeyClass(entityClass), length);
	}

	public static <K extends Serializable> K[] getKeyBuffer(Class<? extends BasicEntity<K>> entityClass) {
		return getKeyBuffer(entityClass, 0);
	}
}
