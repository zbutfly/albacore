package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface VoidFunction<T> extends Serializable {
	public void call(T t);
}
