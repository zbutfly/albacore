package net.butfly.albacore.utils.parallel;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Utils;

public final class Lambdas extends Utils {
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
