package net.butfly.albacore.paral;

import static net.butfly.albacore.paral.Exeters.Internal.DEFEX;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.paral.steam.Steam;

public final class Parals {
	public static int calcBatchParal(long total, long batchSize) {
		return total == 0 ? 0 : (int) (((total - 1) / batchSize) + 1);
	}

	public static long calcBatchSize(long total, int parallelism) {
		return total == 0 ? 0 : (((total - 1) / parallelism) + 1);
	}

	public static Future<?> submit(Runnable task) {
		return DEFEX.submit(task);
	}

	public static Future<?> submit(Runnable... tasks) {
		return DEFEX.submit(tasks);
	}

	public static <T> Future<T> submit(Callable<T> task) {
		return DEFEX.submit(task);
	}

	public static <T> T join(Callable<T> task) {
		return DEFEX.join(task);
	}

	public static void join(Runnable... tasks) {
		DEFEX.join(tasks);
	}

	public static int parallelism() {
		return DEFEX.parallelism();
	}

	public static String status() {
		return DEFEX.toString();
	}

	/** Strict Parallel traversing. */
	public static <R, V> R eachs(Iterable<V> src, Function<V, R> doing, BinaryOperator<R> accumulator) {
		return Steam.of(src.spliterator()).map(doing).reduce(accumulator);
	}

	/** Strict Parallel traversing. */
	public static <R, V> List<R> eachs(Iterable<V> src, Function<V, R> doing) {
		return Steam.of(src.spliterator()).map(doing).list();
	}

	/** Strict Parallel traversing. */
	public static <V> void eachs(Iterable<V> src, Consumer<V> doing) {
		Steam.of(src.spliterator()).each(doing);
	}

	/** Strict Parallel traversing. */
	public static <V> long eachs(Stream<V> src, Consumer<V> doing) {
		return Steam.of(src.spliterator()).map(e -> {
			if (null == e) return 0;
			doing.accept(e);
			return 1;
		}).reduce((i1, i2) -> i1 + i2);
	}
}
