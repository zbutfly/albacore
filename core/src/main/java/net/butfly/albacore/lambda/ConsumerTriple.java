package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface ConsumerTriple<V1, V2, V3> extends Serializable {
	void accept(V1 v1, V2 v2, V3 v3);
}
