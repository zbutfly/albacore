package net.butfly.albacore.support;

import java.io.Serializable;

public interface GenericEnumSupport<E extends GenericEnumSupport<E, V>, V extends Serializable> extends Serializable {
	public static final String ENUM_VALUE_METHOD_NAME = "value";

	V value();
}
