package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Func<T, R> extends Serializable {
	R call(T v);
}
