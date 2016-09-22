package net.butfly.albacore.lambda;

import java.io.Serializable;

import scala.Tuple2;

@FunctionalInterface
public interface PairConverter<T, K, V> extends Serializable, Converter<T, Tuple2<K, V>> {
	public Tuple2<K, V> apply(T t);

	static <K, V> PairConverter<Tuple2<K, V>, K, V> identity() {
		return t -> new Tuple2<>(t._1, t._2);
	}
}
