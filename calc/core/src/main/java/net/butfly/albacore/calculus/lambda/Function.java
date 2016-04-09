package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Function<T1, R> extends Serializable {
	R call(T1 v1);
}
