//package net.butfly.albacore.lambda;
//
//import java.io.Serializable;
//import java.util.Objects;
//import java.util.function.BiFunction;
//
//@FunctionalInterface
//public interface ConverterPair<K, V, R> extends Serializable, BiFunction<K, V, R> {
//	@Override
//	public R apply(K v1, V v2);
//
//	/**
//	 * Returns a composed function that first applies this function to its
//	 * input, and then applies the {@code after} function to the result. If
//	 * evaluation of either function throws an exception, it is relayed to the
//	 * caller of the composed function.
//	 *
//	 * @param <R2>
//	 *            the type of output of the {@code after} function, and of the
//	 *            composed function
//	 * @param after
//	 *            the function to apply after this function is applied
//	 * @return a composed function that first applies this function and then
//	 *         applies the {@code after} function
//	 * @throws NullPointerException
//	 *             if after is null
//	 */
//	default <R2> ConverterPair<K, V, R2> andThen(Converter<? super R, ? extends R2> after) {
//		Objects.requireNonNull(after);
//		return (K k, V v) -> after.apply(apply(k, v));
//	}
//}
