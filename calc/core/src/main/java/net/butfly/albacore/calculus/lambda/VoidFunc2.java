package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface VoidFunc2<V1, V2> extends Serializable {
	void call(V1 v1, V2 v2);
}
