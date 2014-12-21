package net.butfly.albacore.entity;

import java.io.Serializable;
import java.lang.reflect.Array;

import net.butfly.albacore.exception.SystemException;

public abstract class Entity<K extends Serializable> extends AbstractEntity {
	private static final long serialVersionUID = 1L;
	protected K id;
	private String tableName;

	public Entity() {
		super();
		this.tableName = this.generateTableName();
	}

	protected String generateTableName() {
		return this.getClass().getSimpleName().replaceAll("([a-z])([A-Z])", "$1_$2").replaceAll("([A-Z])([A-Z][a-z])", "$1_$2")
				.toUpperCase();
	}

	public String getNamespace() {
		return this.getClass().getName();
	}

	public String getTableName() {
		return tableName;
	}

	public K getId() {
		return id;
	}

	public void setId(K id) {
		this.id = id;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object object) {
		if (object == null || this.getId() == null || !object.getClass().equals(this.getClass())) return false;
		return this.getId().equals(((Entity<K>) object).getId());
	}

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
