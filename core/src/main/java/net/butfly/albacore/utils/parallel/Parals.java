package net.butfly.albacore.utils.parallel;

import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.parallel.Exeters.DEFEX;
import static net.butfly.albacore.utils.parallel.Lambdas.func;

import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

/**
 * <b>Auto detection of thread executor type and parallelism based on <code>-Dalbacore.parallel.factor=factor(double)</code>, default 0.</b>
 * <blockquote>Default <code>factor<code> value without
 * <code>albacore.parallel.factor</code> setting causes traditional unlimited <code>CachedThreadPool</code> implementation.</blockquote>
 * 
 * <ul>
 * <li>Positives double values: ForkJoinPool</li>
 * <blockquote><code>(factor - 1) * (JVM_PARALLELISM - IO_PARALLELISM) + IO_PARALLELISM</code></blockquote>
 * <ul>
 * <li>Minimum: 2</li>
 * <li>IO_PARALLELISM: 16</li>
 * <li>JVM_PARALLELISM: <code>ForkJoinPool.getCommonPoolParallelism()</code></li>
 * </ul>
 * Which means:
 * <ul>
 * <li>1: IO_PARALLELISM</li>
 * <li>(0, 1): less than IO_PARALLELISM</li>
 * <li>(1, 2): (IO_PARALLELISM, JVM_PARALLELISM)</li>
 * <li>2: JVM_PARALLELISM</li>
 * <li>(2, ): more than JVM_PARALLELISM</li>
 * </ul>
 * <li>0: CachedThreadPool</li>
 * <li>Negatives values: FixedThreadPool with parallelism = <code>abs((int)facor)</code></li>
 * </ul>
 * 
 * @author zx
 */
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
