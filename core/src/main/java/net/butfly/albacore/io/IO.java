package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Exceptions;
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

	static <V, A, R> R collect(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return Context.io.collect(col, mapper, collector);
	}

	@Deprecated
	static <V, A, R> R mapping(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return Context.io.mapping(col, mapping, collector);
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

	static <T> ListenableFuture<T> listen(Callable<T> task) {
		return Context.io.listen(task);
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

	@SafeVarargs
	static void run(Runnable... tasks) {
		Context.io.run(tasks);
	}

	static <T, T1, K, V> Map<K, V> map(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
		return Context.io.map(col, mapper, keying, valuing);
	}

	static <T, K, V> Map<K, V> map(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
		return Context.io.map(col, keying, valuing);
	}

	static long sum(Iterable<? extends Future<? extends Number>> futures, Logger errorLogger) {
		long count = 0;
		for (Future<? extends Number> f : futures) {
			Number n;
			try {
				n = f.get();
			} catch (InterruptedException e) {
				errorLogger.error("Batch interrupted");
				continue;
			} catch (ExecutionException e) {
				errorLogger.error("Batch fail", Exceptions.unwrap(e));
				continue;
			}
			if (null != n) count += n.longValue();
		}
		return count;

	}

	public static <V, R> Spliterator<R> split(Spliterator<V> origin, long max, Function<Spliterator<V>, Spliterator<R>> using) {
		List<Future<Spliterator<R>>> fs = new ArrayList<>();
		while (origin.estimateSize() > max) {
			Spliterator<V> split = origin.trySplit();
			if (null != split) {
				fs.add(Context.io.executor.submit(() -> using.apply(split)));
			}
		}
		fs.add(Context.io.executor.submit(() -> using.apply(origin)));
		List<Spliterator<R>> rs = IO.list(fs, f -> {
			try {
				return f.get();
			} catch (InterruptedException | ExecutionException e) {
				return Spliterators.emptySpliterator();
			}
		});
		return merge(rs);
	}

	static <V> Spliterator<V> merge(Iterable<Spliterator<V>> rs) {
		long size = Streams.of(rs).collect(Collectors.summingLong(i -> i.estimateSize()));
		Iterator<Spliterator<V>> it = rs.iterator();
		if (!it.hasNext()) return Spliterators.emptySpliterator();

		return new Spliterator<V>() {
			@Override
			public boolean tryAdvance(Consumer<? super V> action) {
				return false;
			}

			@Override
			public Spliterator<V> trySplit() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long estimateSize() {
				return size;
			}

			@Override
			public int characteristics() {
				return 0;
			}
		};
	}
}
