package net.butfly.albacore.lambda;

import java.util.function.BiConsumer;

import net.butfly.albacore.utils.Pair;

public interface ConsumerPair<K, V> extends BiConsumer<K, V>, Consumer<Pair<K, V>> {
	@Override
	void accept(K v1, V v2);

	@Override
	default void accept(Pair<K, V> t) {
		accept(t.v1(), t.v2());
	}
}
