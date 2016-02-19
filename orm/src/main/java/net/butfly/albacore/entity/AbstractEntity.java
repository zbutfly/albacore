package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.Beans;
import net.butfly.albacore.utils.Generics;

public interface AbstractEntity<K extends Serializable> extends Beans<AbstractEntity<K>> {
	K getId();

	void setId(K id);

	public static <KK extends Serializable> Class<KK> getKeyClass(Class<? extends AbstractEntity<?>> entityClass) {
		return Generics.resolveGenericParameter(entityClass, AbstractEntity.class, "K");
	}
}
