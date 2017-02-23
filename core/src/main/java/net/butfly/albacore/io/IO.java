package net.butfly.albacore.io;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

public interface IO extends Sizable, Openable {
	static final String PARALLELISM_RATIO_KEY = "albacore.io.parallelism.ratio";

	class Context {
		final static String EXECUTOR_NAME = "Albacore-IO-Streaming";

		private static int calcParallelism() {
			Logger logger = Logger.getLogger(Context.class);
			if (Configs.MAIN_CONF.containsKey(PARALLELISM_RATIO_KEY)) {
				double r = Double.parseDouble(Configs.MAIN_CONF.get(PARALLELISM_RATIO_KEY));
				int p = 16 + (int) Math.round((ForkJoinPool.getCommonPoolParallelism() - 16) * (r - 1));
				if (p < 2) p = 2;
				logger.info("AlbacoreIO parallelism calced as: " + p + " [from: (((-D" + PARALLELISM_RATIO_KEY + "(" + r
						+ ")) - 1) * (JVM_DEFAULT_PARALLELISM(" + ForkJoinPool.getCommonPoolParallelism()
						+ ") - IO_DEFAULT_PARALLELISM(16))) + IO_DEFAULT_PARALLELISM(16), Max=JVM_DEFAULT_PARALLELISM, Min=2]");
				return p;
			} else {
				logger.info("AlbacoreIO use traditional cached thread pool.");
				return 0;
			}
		}

		final private static StreamExecutor io = new StreamExecutor(EXECUTOR_NAME, calcParallelism(), false);
	}

	long size();

	static <V, A, R> R map(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return Context.io.map(col, mapper, collector);
	}

	static <V, A, R> R collect(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return Context.io.collect(col, mapping, collector);
	}

	static <V, R> R collect(Iterable<? extends V> col, Collector<V, ?, R> collector) {
		return Context.io.collect(col, collector);
	}

	static <V, R> R collect(Stream<? extends V> stream, Collector<V, ?, R> collector) {
		return Context.io.collect(stream, collector);
	}

	static <V> List<V> list(Stream<V> stream) {
		return Context.io.list(stream);
	}

	static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return Context.io.list(col, mapper);
	}

	static <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		Context.io.each(col, consumer);
	}

	static String tracePool(String prefix) {
		return Context.io.tracePool(prefix);
	}

	static <T> ListenableFuture<List<T>> listen(List<Callable<T>> tasks) {
		return Context.io.listen(tasks);
	}

	static ListenableFuture<List<Object>> listenRun(List<Runnable> tasks) {
		return Context.io.listenRun(tasks);
	}

	static int parallelism() {
		return Context.io.parallelism();
	}

	static <T> T run(Callable<T> task) {
		return Context.io.run(task);
	}

	@SafeVarargs
	static <T> List<T> run(Callable<T>... tasks) {
		return Context.io.run(tasks);
	}
}
