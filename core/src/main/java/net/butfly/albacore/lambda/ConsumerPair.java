package net.butfly.albacore.lambda;

import java.util.function.BiConsumer;

import scala.Tuple2;

public interface ConsumerPair<K, V> extends BiConsumer<K, V>, Consumer<Tuple2<K, V>> {
	@Override
	void accept(K v1, V v2);

	@Override
	default void accept(Tuple2<K, V> t) {
		accept(t._1, t._2);
	}
}
