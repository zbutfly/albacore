package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Supplier<T> extends Serializable, java.util.function.Supplier<T> {
	@Override
	T get();
}
