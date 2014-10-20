package net.butfly.albacore.dbo.mongo;

import java.io.Serializable;

import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.GenericUtils;

import org.mongodb.morphia.annotations.Id;

public class GenericEntity<K extends Serializable> extends AbstractEntity {
	private static final long serialVersionUID = 3105047393832057088L;
	@Id private K id;

	public K getId() {
		return id;
	}

	public void setId(K id) {
		this.id = id;
	}

	public static Class<?> getKeyClass(Class<? extends GenericEntity<?>> entityClass) {
		try {
			return GenericUtils.getDeclaredField(entityClass, "id").getType();
		} catch (SecurityException e) {
			throw new SystemException("", e);
		} catch (Throwable e) {
			throw new SystemException("", e);
		}
	}
}
