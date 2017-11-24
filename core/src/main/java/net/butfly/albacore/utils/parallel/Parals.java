package net.butfly.albacore.utils.parallel;

import static net.butfly.albacore.paral.Exeters.DEFEX;
import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.parallel.Lambdas.func;

import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

@Deprecated
public final class Parals extends Utils {
	private static final Logger logger = Logger.getLogger(Parals.class);

	public static int calcBatchParal(long total, long batch) {
		return total == 0 ? 0 : (int) (((total - 1) / batch) + 1);
	}

	public static long calcBatchSize(long total, int parallelism) {
		return total == 0 ? 0 : (((total - 1) / parallelism) + 1);
	}

	@SuppressWarnings("rawtypes")
	@FunctionalInterface
	private static interface FutureCallback extends com.google.common.util.concurrent.FutureCallback {
		@Override
		default void onFailure(Throwable t) {
			logger.error("Run sequential fail", t);
		}
	}

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @param accumulator
	 * @return
	 */
	public static <R, V> R eachs(Iterable<V> src, Function<V, R> doing, BinaryOperator<R> accumulator) {
		return map(src, doing::apply, Collectors.reducing(null, accumulator));
	}

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @return
	 */
	public static <R, V> List<R> eachs(Iterable<V> src, Function<V, R> doing) {
		return map(src, doing::apply, Collectors.toList());
	}

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @return couting
	 */
	public static <V> void eachs(Iterable<V> src, Consumer<V> doing) {
		map(src, func(doing), Collectors.counting());
	}

	public static <V> long eachs(Stream<V> src, Consumer<V> doing) {
		return map(src, func(doing), Collectors.counting());
	}

	@Deprecated
	public static int parallelism() {
		return DEFEX.parallelism();
	}

	public static String status() {
		return DEFEX.toString();
	}
}
