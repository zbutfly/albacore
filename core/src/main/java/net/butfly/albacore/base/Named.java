package net.butfly.albacore.base;

public interface Named {
	default String name() {
		return getClass().getSimpleName() + "@" + hashCode();
	}
}
