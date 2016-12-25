package net.butfly.albacore.lambda;

import java.io.Serializable;

import scala.Tuple2;

@FunctionalInterface
public interface SupplierPair<K, V> extends Serializable, Supplier<Tuple2<K, V>> {
	@Override
	Tuple2<K, V> get();
}
