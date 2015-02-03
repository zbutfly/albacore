package net.butfly.albacore.utils;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.entity.AbstractEntity;

public final class Entities extends Utils {
	private static final Map<Class<? extends AbstractEntity<?>>, Class<? extends Serializable>> KEY_TYPE_POOL = new ConcurrentHashMap<Class<? extends AbstractEntity<?>>, Class<? extends Serializable>>();

	@SuppressWarnings("unchecked")
	public static <K extends Serializable> Class<K> getKeyClass(Class<? extends AbstractEntity<?>> entityClass) {
		Class<K> keyType = (Class<K>) KEY_TYPE_POOL.get(entityClass);
		if (null == keyType) {
			keyType = Generics.getGenericParamClass(entityClass, AbstractEntity.class, "K");
			KEY_TYPE_POOL.put(entityClass, keyType);
		}
		return keyType;
	}
}
