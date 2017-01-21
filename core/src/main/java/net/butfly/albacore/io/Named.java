package net.butfly.albacore.io;

public interface Named {
	default String name() {
		return getClass().getSimpleName();
	}
}
