package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface VoidFunc<T> extends Serializable {
	public void call(T t);
}
