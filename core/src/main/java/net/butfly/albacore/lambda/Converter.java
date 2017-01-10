package net.butfly.albacore.lambda;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import net.butfly.albacore.utils.Collections;

@FunctionalInterface
public interface Converter<T, R> extends Serializable, Function<T, R> {
	@Override
	R apply(T v);

	default <V> Converter<V, R> compose(Converter<? super V, ? extends T> before) {
		Objects.requireNonNull(before);
		return (V v) -> apply(before.apply(v));
	}

	default <V> Converter<T, V> andThen(Converter<? super R, ? extends V> after) {
		Objects.requireNonNull(after);
		return (T t) -> after.apply(apply(t));
	}

	static <T> Converter<T, T> identity() {
		return t -> t;
	}

	static <T, R> Converter<List<T>, List<R>> list(Converter<T, R> single) {
		return ts -> {
			try {
				return Collections.transform(ts, t -> single.apply(t));
			} catch (Exception ex) {
				return new ArrayList<>();
			}
		};
	}
}
