package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.Beanable;

public interface AbstractEntity<K extends Serializable> extends Beanable<AbstractEntity<K>> {
	K getId();

	void setId(K id);
}
