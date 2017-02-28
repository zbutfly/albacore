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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.utils.Configs;
import static net.butfly.albacore.utils.Exceptions.*;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.logger.Logger;

public interface IO extends Sizable, Openable {
	static final String PARALLELISM_RATIO_KEY = "albacore.io.parallelism.ratio";

	class Context implements Loggable {
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

		final private static StreamExecutor io;
		static {
			io = new StreamExecutor(EXECUTOR_NAME, calcParallelism(), false);
			Systems.handleSignal(sig -> io.close(), "TERM", "INT");
		}

		private static class ConcatSpliterator<V> implements Spliterator<V> {
			private final Iterator<Spliterator<V>> it;
			private final AtomicLong estimateSize;
			private Spliterator<V> curr, next;

			private ConcatSpliterator(Iterable<Spliterator<V>> splits) {
				super();
				it = splits.iterator();
				curr = it.next();
				next = it.hasNext() ? it.next() : null;
				long s = 0;
				for (Spliterator<V> it : splits) {
					long s0 = it.estimateSize();
					if (s0 == Long.MAX_VALUE || (s += s0) < -1) {
						s = Long.MAX_VALUE;
						break;
					}
				}
				estimateSize = s < Long.MAX_VALUE ? new AtomicLong(s) : null;
			}

			@Override
			public boolean tryAdvance(Consumer<? super V> using) {
				boolean used;
				while (!(used = curr.tryAdvance(using))) {
					if (null != next) {
						curr = next;
						next = it.hasNext() ? it.next() : null;
					} else return false;
				}
				if (used && estimateSize != null) estimateSize.decrementAndGet();
				return used;
			}

			@Override
			public Spliterator<V> trySplit() {
				if (null != next) {
					Spliterator<V> s = next;
					next = it.hasNext() ? it.next() : null;
					return s;
				}
				return curr.trySplit();
			}

			@Override
			public long estimateSize() {
				return estimateSize.get();
			}

			@Override
			public int characteristics() {
				return curr.characteristics();
			}
		}
	}

	// parallel
	static <T> T run(Callable<T> task) {
		return Context.io.run(task);
	}

	static void run(Runnable task) {
		Context.io.run(task);
	}

	static void run(Runnable... tasks) {
		Context.io.run(tasks);
	}

	static <T> List<T> run(List<Callable<T>> tasks) {
		return Context.io.run(tasks);
	}

	static <T> ListenableFuture<List<T>> listen(List<Callable<T>> tasks) {
		return Context.io.listen(tasks);
	}

	static <T> ListenableFuture<T> listen(Callable<T> task) {
		return Context.io.listen(task);
	}

	static ListenableFuture<List<Object>> listenRun(Runnable... tasks) {
		return Context.io.listenRun(tasks);
	}

	static ListenableFuture<?> listenRun(Runnable task) {
		return Context.io.listenRun(task);
	}

	// ex parallel
	public static <V> void split(Spliterator<V> origin, long max, Consumer<Spliterator<V>> using) {
		List<Future<?>> fs = new ArrayList<>();
		while (origin.estimateSize() > max) {
			Spliterator<V> split = origin.trySplit();
			if (null != split) fs.add(Context.io.executor.submit(() -> split(split, max, using)));
		}
		if (origin.estimateSize() > 0) using.accept(origin);
		for (Future<?> f : fs) {
			try {
				f.get();
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				StreamExecutor.logger.error("Subtask error", unwrap(e));
			}
		}
	}

	public static <V, R> Spliterator<R> splitv(Spliterator<V> origin, long max, Function<Spliterator<V>, Spliterator<R>> using) {
		List<Future<Spliterator<R>>> fs = new ArrayList<>();
		while (origin.estimateSize() > max) {
			Spliterator<V> split = origin.trySplit();
			if (null != split) fs.add(Context.io.executor.submit(() -> splitv(split, max, using)));
		}
		Spliterator<R> v = origin.estimateSize() > 0 ? using.apply(origin) : null;
		List<Spliterator<R>> rs = IO.list(fs, f -> {
			try {
				return f.get();
			} catch (InterruptedException | ExecutionException e) {
				return Spliterators.emptySpliterator();
			}
		});
		if (null != v) rs.add(0, v);
		return new Context.ConcatSpliterator<>(rs);
	}

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @param accumulator
	 * @return
	 */
	default <R, V> R eachs(Iterable<V> src, Function<V, R> doing, BinaryOperator<R> accumulator) {
		List<ListenableFuture<R>> fs = new ArrayList<>();
		src.forEach(v -> fs.add(IO.listen(() -> doing.apply(v))));
		if (fs.isEmpty()) return null;
		List<R> rs = new ArrayList<>();
		R r;
		for (ListenableFuture<R> f : fs)
			try {
				r = f.get();
				if (null != r) rs.add(r);
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				logger().error("Subtask error", unwrap(e));
			}
		return rs.parallelStream().collect(Collectors.reducing(null, accumulator));
	}

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @param accumulator
	 * @return
	 */
	default <V> void eachs(Iterable<V> src, Consumer<V> doing) {
		List<ListenableFuture<?>> fs = new ArrayList<>();
		src.forEach(v -> fs.add(IO.listenRun(() -> doing.accept(v))));
		for (ListenableFuture<?> f : fs)
			try {
				f.get();
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				logger().error("Subtask error", unwrap(e));
			}
	}

	// mapping
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
				errorLogger.error("Batch fail", unwrap(e));
				continue;
			}
			if (null != n) count += n.longValue();
		}
		return count;

	}

	// status
	static int parallelism() {
		return Context.io.parallelism();
	}

	static String tracePool(String prefix) {
		return Context.io.tracePool(prefix);
	}
}
