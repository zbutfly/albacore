package net.butfly.albacore.support;

public interface EnumSupport<E extends EnumSupport<E>> extends GenericEnumSupport<E, Integer> {
	Integer value();
}
