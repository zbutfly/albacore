package net.butfly.albacore.utils.parallel;

import java.util.ArrayList;
import java.util.List;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.io.lambda.Supplier;

import net.butfly.albacore.utils.Utils;

public final class Lambdas extends Utils {
	@SuppressWarnings("unchecked")
	public final static <N extends Number> BinaryOperator<N> sumInt() {
		return (i1, i2) -> {
			int i;
			if (null == i1 && null == i2) i = 0;
			else if (null == i1) i = i2.intValue();
			else if (null == i2) i = i1.intValue();
			else i = i1.intValue() + i2.intValue();
			return (N) Integer.valueOf(i);
		};
	}

	@SuppressWarnings("unchecked")
	public final static <N extends Number> BinaryOperator<N> sumLong() {
		return (i1, i2) -> {
			long v1 = null == i1 ? 0 : i1.longValue();
			long v2 = null == i1 ? 0 : i1.longValue();
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
