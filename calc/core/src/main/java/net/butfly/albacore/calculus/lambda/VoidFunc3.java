package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface VoidFunc3<V1, V2, V3> extends Serializable {
	void call(V1 v1, V2 v2, V3 v3);
}
