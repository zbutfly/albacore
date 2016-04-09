package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

import scala.Tuple2;

@FunctionalInterface
public interface PairFunction<T, K, V> extends Serializable {
	public Tuple2<K, V> call(T t);
}
