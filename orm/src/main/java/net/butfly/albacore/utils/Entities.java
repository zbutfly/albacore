package net.butfly.albacore.utils;

import java.io.Serializable;

import net.butfly.albacore.entity.AbstractEntity;

public final class Entities extends Utils {
	public static <K extends Serializable> Class<K> getKeyClass(Class<? extends AbstractEntity<?>> entityClass) {
		return Generics.getGenericParamClass(entityClass, AbstractEntity.class, "K");
	}
}
