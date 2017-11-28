package net.butfly.albacore.utils.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Utils;

public final class Lambdas extends Utils {
	@SuppressWarnings("unchecked")
	public final static <N extends Number> BinaryOperator<N> sumInt() {
		return (i1, i2) -> {
			int v1 = null == i1 ? 0 : i1.intValue();
			int v2 = null == i1 ? 0 : i1.intValue();
			return (N) (Integer.valueOf(v1 + v2));
		};
	}

	@SuppressWarnings("unchecked")
	public final static <N extends Number> BinaryOperator<N> sumLong() {
		return (i1, i2) -> {
			long v1 = null == i1 ? 0 : i1.intValue();
			long v2 = null == i1 ? 0 : i1.intValue();
			return (N) (Long.valueOf(v1 + v2));
		};
	}

	public static <V> Predicate<V> notNull() {
		return v -> null != v;
	}

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

	public static <T> BinaryOperator<List<T>> merging() {
		return (t1, t2) -> {
			List<T> l = new ArrayList<>();
			l.addAll(t1);
			l.addAll(t2);
			return l;
		};
	}
}
