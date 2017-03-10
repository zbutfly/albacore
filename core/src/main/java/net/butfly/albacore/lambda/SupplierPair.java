package net.butfly.albacore.lambda;

import java.io.Serializable;

import net.butfly.albacore.utils.Pair;

@FunctionalInterface
public interface SupplierPair<K, V> extends Serializable, Supplier<Pair<K, V>> {
	@Override
	Pair<K, V> get();
}
