//package net.butfly.albacore.lambda;
//
//import java.io.Serializable;
//
//import net.butfly.albacore.utils.Pair;
//
//@FunctionalInterface
//public interface PairConverter<T, K, V> extends Serializable, Converter<T, Pair<K, V>> {
//	static <K, V> PairConverter<Pair<K, V>, K, V> identity() {
//		return t -> new Pair<>(t.v1(), t.v2());
//	}
//}
