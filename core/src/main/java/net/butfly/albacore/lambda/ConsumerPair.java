package net.butfly.albacore.lambda;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface ConsumerPair<K, V> extends Serializable, BiConsumer<K, V> {
	void accept(K v1, V v2);
}
