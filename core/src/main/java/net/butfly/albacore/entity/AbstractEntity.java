package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.Beans;

public interface AbstractEntity<K extends Serializable> extends Beans<AbstractEntity<K>> {
	K getId();

	void setId(K id);
}
