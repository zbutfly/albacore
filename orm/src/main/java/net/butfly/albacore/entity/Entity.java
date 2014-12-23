package net.butfly.albacore.entity;

import java.io.Serializable;
import java.lang.reflect.Array;

import net.butfly.albacore.exception.SystemException;

public interface Entity<K extends Serializable> extends AbstractEntity {
	K getId();

	void setId(K id);

	public static Class<?> getKeyClass(Class<? extends Entity<?>> entityClass) {
		try {
			return entityClass.getMethod("getId").getReturnType();
		} catch (SecurityException e) {
			throw new SystemException("", e);
		} catch (Throwable e) {
			throw new SystemException("", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <K extends Serializable> K[] getKeyBuffer(Class<? extends Entity<K>> entityClass, int length) {
		return (K[]) Array.newInstance(getKeyClass(entityClass), length);
	}

	public static <K extends Serializable> K[] getKeyBuffer(Class<? extends BasicEntity<K>> entityClass) {
		return getKeyBuffer(entityClass, 0);
	}
}
