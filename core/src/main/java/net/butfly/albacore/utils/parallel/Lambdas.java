package net.butfly.albacore.utils.parallel;

import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Utils;

public final class Lambdas extends Utils {
	public final static BinaryOperator<Number> sumInt = (i1, i2) -> (null == i1 ? 0 : i1.intValue()) + (null == i2 ? 0 : i2.intValue());

	public static <T> BinaryOperator<T> nullOr() {
		return (v1, v2) -> null == v1 ? v2 : v1;
	}

	public static <V> Function<V, Void> func(Consumer<V> c) {
		return v -> {
			c.accept(v);
			return null;
		};
	}

	public static <V> Function<Void, V> func(Supplier<V> c) {
		return v -> c.get();
	}
}
