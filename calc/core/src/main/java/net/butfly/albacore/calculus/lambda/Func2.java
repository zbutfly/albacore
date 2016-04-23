package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Func2<T1, T2, R> extends Serializable {
	public R call(T1 v1, T2 v2);
}
