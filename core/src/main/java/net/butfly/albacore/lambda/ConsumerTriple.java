package net.butfly.albacore.lambda;

import scala.Tuple3;

@FunctionalInterface
public interface ConsumerTriple<V1, V2, V3> extends Consumer<Tuple3<V1, V2, V3>> {
	void accept(V1 v1, V2 v2, V3 v3);

	@Override
	default void accept(Tuple3<V1, V2, V3> t) {
		accept(t._1(), t._2(), t._3());
	}
}
