package net.butfly.albacore.io.utils;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;

public final class Parals extends Utils {
	private static final Logger logger = Logger.getLogger(Parals.class);

	public static final class Pool<V> {
		private final LinkedBlockingQueue<V> pool;
		final Supplier<V> constuctor;
		final Consumer<V> destroyer;

		public Pool(int size, Supplier<V> constuctor) {
			this(size, constuctor, v -> {});
		}

		public Pool(int size, Supplier<V> constuctor, Consumer<V> destroyer) {
			pool = new LinkedBlockingQueue<>(size);
			this.constuctor = constuctor;
			this.destroyer = destroyer;
		}

		public void use(Consumer<V> using) {
			V v = pool.poll();
			if (null == v) v = constuctor.get();
			try {
				using.accept(v);
			} finally {
				if (!pool.offer(v) && v instanceof AutoCloseable) try {
					((AutoCloseable) v).close();
				} catch (Exception e) {}
			}
		}
	}

	private final static String EXECUTOR_NAME = "AlbacoreIOStream";
	private static final String PARALLELISM_FACTOR_KEY = "albacore.io.stream.parallelism.factor";

	private static int calcParallelism() {
		int p = 0;
		if (Configs.has(PARALLELISM_FACTOR_KEY)) {
			double r = Double.parseDouble(Configs.gets(PARALLELISM_FACTOR_KEY));
			p = 16 + (int) Math.round((ForkJoinPool.getCommonPoolParallelism() - 16) * (r - 1));
			if (p < 2) p = 2;
			int pa = p;
			logger.debug(() -> "AlbacoreIO parallelism calced as: [" + pa + "]\n\t[from: (((-D" + PARALLELISM_FACTOR_KEY + "(" + r
					+ ")) - 1) * (JVM_DEFAULT_PARALLELISM(" + ForkJoinPool.getCommonPoolParallelism()
					+ ") - IO_DEFAULT_PARALLELISM(16))) + IO_DEFAULT_PARALLELISM(16), Max=JVM_DEFAULT_PARALLELISM, Min=2]");
		} else logger.info("AlbacoreIO use traditional cached thread pool.");
		return p;
	}

	final private static Exor io = new Exor();

	public static int calcBatchParal(long total, long batch) {
		return total == 0 ? 0 : (int) (((total - 1) / batch) + 1);
	}

	public static long calcBatchSize(long total, int parallelism) {
		return total == 0 ? 0 : (((total - 1) / parallelism) + 1);
	}

	private final static class Exor extends Namedly implements AutoCloseable {
		public static final Logger logger = Logger.getLogger(Exor.class);
		final ExecutorService exor;
		final ListeningExecutorService lexor;
		private final static Map<String, ThreadGroup> g = new ConcurrentHashMap<>();

		public Exor() {
			this(EXECUTOR_NAME, calcParallelism(), false);
		}

		public Exor(String name, int parallelism, boolean throwException) {
			exor = parallelism < 1 ? Executors.newCachedThreadPool(r -> new Thread(g.computeIfAbsent(name, n -> new ThreadGroup(name
					+ "ThreadGroup")), r, name + "@" + Texts.formatDate(new Date())))
					: Concurrents.executorForkJoin(parallelism, name, (t, e) -> {
						logger.error("Migrater pool task failure @" + t.getName(), e);
						if (throwException) throw wrap(unwrap(e));
					});
			if (exor instanceof ThreadPoolExecutor) ((ThreadPoolExecutor) exor).setRejectedExecutionHandler((r, ex) -> logger.error(
					tracePool("Task rejected by the exor")));
			lexor = MoreExecutors.listeningDecorator(exor);
			Systems.handleSignal(sig -> close(), "TERM", "INT");
		}

		@SuppressWarnings("rawtypes")
		@FunctionalInterface
		private interface FutureCallback extends com.google.common.util.concurrent.FutureCallback {
			@Override
			default void onFailure(Throwable t) {
				logger.error("Run sequential fail", t);
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void runs(Runnable... firstAndThens) {
			if (null == firstAndThens || firstAndThens.length == 0) return;
			ListenableFuture f = lexor.submit(firstAndThens[0]);
			if (firstAndThens.length > 1) f.addListener(() -> runs(Arrays.copyOfRange(firstAndThens, 1, firstAndThens.length)), exor);
			get(f);
		}

		public void run(Runnable... tasks) {
			get(listenRun(tasks));
		}

		public void run(Runnable task) {
			get(listenRun(task));
		}

		public <T> void runs(Callable<T> first, Consumer<T> then) {
			if (null == first) return;
			ListenableFuture<T> f = lexor.submit(first);
			if (then != null) f.addListener(() -> {
				try {
					then.accept(f.get());
				} catch (InterruptedException e) {} catch (ExecutionException e) {
					logger.error("Subtask error", unwrap(e));
				}
			}, exor);
			get(f);
		}

		public <T> T run(Callable<T> task) {
			return get(listen(task));
		}

		public <T> List<T> run(List<Callable<T>> tasks) {
			return get(listen(tasks));
		}

		public <T> ListenableFuture<List<T>> listen(List<? extends Callable<T>> tasks) {
			return Futures.successfulAsList(list(tasks, this::listen));
		}

		public <T> ListenableFuture<T> listen(Callable<T> task) {
			try {
				return lexor.submit(task);
			} catch (RejectedExecutionException e) {
				logger.error("Rejected");
				throw e;
			}
		}

		public ListenableFuture<List<Object>> listenRun(Runnable... tasks) {
			return Futures.successfulAsList(list(Arrays.asList(tasks), this::listenRun));
		}

		public ListenableFuture<?> listenRun(Runnable task) {
			try {
				return lexor.submit(task);
			} catch (RejectedExecutionException e) {
				logger.error("Rejected");
				throw e;
			}
		}

		public <V, A, R> R collect(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
			return collect(Streams.of(col).map(mapper), collector);
		}

		public <T, T1, K, V> Map<K, V> map(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
			return collect(col.map(mapper), Collectors.toMap(keying, valuing));
		}

		public <T, K, V> Map<K, V> map(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
			return collect(col, Collectors.toMap(keying, valuing));
		}

		public <V, A, R> R mapping(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
			return collect(mapping.apply(Streams.of(col)), collector);
		}

		public <V, R> R collect(Iterable<? extends V> col, Collector<? super V, ?, R> collector) {
			return collect(Streams.of(col), collector);
		}

		private static final boolean STREAM_DEBUGGING = Boolean.parseBoolean(System.getProperty("albacore.io.stream.debug", "false"));

		public <V, R> R collect(Stream<? extends V> s, Collector<? super V, ?, R> collector) {
			if (STREAM_DEBUGGING && logger.isTraceEnabled()) {
				AtomicLong c = new AtomicLong();
				R r = Streams.of(s).peek(e -> c.incrementAndGet()).collect(collector);
				logger.debug("One stream collected [" + c.get() + "] elements, performance issue?");
				return r;
			} else return Streams.of(s).collect(collector);
		}

		public <V> List<V> list(Stream<? extends V> stream) {
			return collect(Streams.of(stream), Collectors.toList());
		}

		public <V, R> List<R> list(Stream<V> stream, Function<V, R> mapper) {
			return collect(Streams.of(stream).map(mapper), Collectors.toList());
		}

		public <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
			return collect(col, mapper, Collectors.toList());
		}

		public <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
			each(Streams.of(col), consumer);
		}

		private <V> void each(Stream<V> s, Consumer<? super V> consumer) {
			get(Futures.successfulAsList(list(Streams.of(s).map(v -> listenRun(() -> consumer.accept(v))))));
		}

		public String tracePool(String prefix) {
			if (exor instanceof ForkJoinPool) {
				ForkJoinPool ex = (ForkJoinPool) exor;
				return MessageFormat.format("{5}, Fork/Join: tasks={4}, threads(active/running)={1}/{2}, steals={3}, pool size={0}", ex
						.getPoolSize(), ex.getActiveThreadCount(), ex.getRunningThreadCount(), ex.getStealCount(), ex.getQueuedTaskCount(),
						prefix);
			} else if (exor instanceof ThreadPoolExecutor) {
				ThreadPoolExecutor ex = (ThreadPoolExecutor) exor;
				return MessageFormat.format("{3}, ThreadPool: tasks={2}, threads(active)={1}, pool size={0}", ex.getPoolSize(), ex
						.getActiveCount(), ex.getTaskCount(), prefix);
			} else return prefix + ": " + exor.toString();
		}

		@Override
		public void close() {}

		public int parallelism() {
			return exor instanceof ForkJoinPool ? ((ForkJoinPool) exor).getParallelism() : 0;
		}

	}

	// parallel
	public static <T> T run(Callable<T> task) {
		return io.run(task);
	}

	public static void run(Runnable task) {
		io.run(task);
	}

	public static void run(Runnable... tasks) {
		io.run(tasks);
	}

	public static void runs(Runnable... tasks) {
		io.runs(tasks);
	}

	public static <T> List<T> run(List<Callable<T>> tasks) {
		return io.run(tasks);
	}

	public static <T> void runs(Callable<T> first, Consumer<T> then) {
		io.runs(first, then);
	}

	public static <T> ListenableFuture<List<T>> listen(List<Callable<T>> tasks) {
		return io.listen(tasks);
	}

	public static <T> ListenableFuture<T> listen(Callable<T> task) {
		return io.listen(task);
	}

	public static ListenableFuture<List<Object>> listenRun(Runnable... tasks) {
		return io.listenRun(tasks);
	}

	public static ListenableFuture<?> listenRun(Runnable task) {
		return io.listenRun(task);
	}

	// ex parallel

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @param accumulator
	 * @return
	 */
	public static <R, V> R eachs(Iterable<V> src, Function<V, R> doing, BinaryOperator<R> accumulator) {
		List<ListenableFuture<R>> fs = new ArrayList<>();
		src.forEach(v -> fs.add(listen(() -> doing.apply(v))));
		if (fs.isEmpty()) return null;
		List<R> rs = new ArrayList<>();
		R r;
		for (ListenableFuture<R> f : fs)
			try {
				r = f.get();
				if (null != r) rs.add(r);
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				logger.error("Subtask error", unwrap(e));
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
	public static <V> long eachs(Iterable<V> src, Consumer<V> doing) {
		List<ListenableFuture<?>> fs = new ArrayList<>();
		src.forEach(v -> fs.add(listenRun(() -> doing.accept(v))));
		for (ListenableFuture<?> f : fs)
			try {
				f.get();
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				logger.error("Subtask error", unwrap(e));
			}
		return fs.size();
	}

	// mapping
	public static <V, A, R> R collect(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return io.collect(col, mapper, collector);
	}

	@Deprecated
	public static <V, A, R> R mapping(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return io.mapping(col, mapping, collector);
	}

	public static <V, R> R collect(Iterable<? extends V> col, Collector<V, ?, R> collector) {
		return io.collect(col, collector);
	}

	public static <V, R> R collect(Stream<? extends V> stream, Collector<V, ?, R> collector) {
		return io.collect(stream, collector);
	}

	public static <V> List<V> list(Stream<V> stream) {
		return io.list(stream);
	}

	public static <V, R> List<R> list(Stream<V> stream, Function<V, R> mapper) {
		return io.list(stream, mapper);
	}

	public static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return io.list(col, mapper);
	}

	public static <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		io.each(col, consumer);
	}

	public static <T, T1, K, V> Map<K, V> map(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
		return io.map(col, mapper, keying, valuing);
	}

	public static <T, K, V> Map<K, V> map(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
		return io.map(col, keying, valuing);
	}

	public static long sum(Iterable<? extends Future<? extends Number>> futures, Logger errorLogger) {
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
	public static int parallelism() {
		return io.parallelism();
	}

	public static String tracePool(String prefix) {
		return io.tracePool(prefix);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void join(Future... futures) {
		for (Future f : futures)
			get(f);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void join(Iterable<Future<?>> futures) {
		for (Future f : futures)
			get(f);
	}

	public static <T> T get(Future<T> f) {
		try {
			return f.get();
		} catch (InterruptedException e) {} catch (ExecutionException e) {
			logger.error("Subtask error", unwrap(e));
		}
		return null;
	}
}
