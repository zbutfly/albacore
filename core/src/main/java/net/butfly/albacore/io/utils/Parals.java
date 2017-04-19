package net.butfly.albacore.io.utils;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.lang.Thread.UncaughtExceptionHandler;
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
import java.util.concurrent.ThreadFactory;
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

/**
 * <b>Auto detection of thread executor type and parallelism based on
 * <code>-Dalbacore.io.stream.parallelism.factor=factor(double)</code>, default
 * 0.</b> <blockquote>Default <code>factor<code> value without
 * <code>albacore.io.stream.parallelism.factor</code> setting causes traditional
 * unlimited <code>CachedThreadPool</code> implementation.</blockquote>
 * 
 * <ul>
 * <li>Positives double values: ForkJoinPool</li>
 * <blockquote><code>(factor - 1) * (JVM_PARALLELISM - IO_PARALLELISM) + IO_PARALLELISM</code></blockquote>
 * <ul>
 * <li>Minimum: 2</li>
 * <li>IO_PARALLELISM: 16</li>
 * <li>JVM_PARALLELISM:
 * <code>ForkJoinPool.getCommonPoolParallelism()</code></li>
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
 * <li>Negatives values: FixedThreadPool with parallelism =
 * <code>abs((int)facor)</code></li>
 * </ul>
 * 
 * @author zx
 */
public final class Parals extends Utils {
	private static final Logger logger = Logger.getLogger(Parals.class);
	private final static String EXECUTOR_NAME = "AlbacoreIOStream";
	private static final String PARALLELISM_FACTOR_KEY = "albacore.io.stream.parallelism.factor";
	private static final boolean STREAM_DEBUGGING = Boolean.parseBoolean(System.getProperty("albacore.io.stream.debug", "false"));
	private final static int SYS_PARALLELISM = Exers.detectParallelism();

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void runs(Runnable... firstAndThens) {
		if (null == firstAndThens || firstAndThens.length == 0) return;
		ListenableFuture f = EXERS.lexor.submit(firstAndThens[0]);
		if (firstAndThens.length > 1) f.addListener(() -> runs(Arrays.copyOfRange(firstAndThens, 1, firstAndThens.length)), EXERS.exor);
		get(f);
	}

	public static void run(Runnable... tasks) {
		get(listenRun(tasks));
	}

	public static void run(Runnable task) {
		get(listenRun(task));
	}

	@SafeVarargs
	public static <T> void runss(Callable<T> first, Consumer<T>... then) {
		if (null == first) return;
		ListenableFuture<T> f = EXERS.lexor.submit(first);
		for (Consumer<T> t : then)
			if (t != null) f.addListener(() -> {
				try {
					t.accept(f.get());
				} catch (InterruptedException e) {} catch (ExecutionException e) {
					logger.error("Subtask error", unwrap(e));
				}
			}, EXERS.exor);
		get(f);
	}

	public static <T> T run(Callable<T> task) {
		return get(listen(task));
	}

	@SafeVarargs
	public static <T> List<T> run(Callable<T>... tasks) {
		return get(listen(Arrays.asList(tasks)));
	}

	public static <T> List<T> run(List<Callable<T>> tasks) {
		return get(listen(tasks));
	}

	public static <T> ListenableFuture<List<T>> listen(List<? extends Callable<T>> tasks) {
		return Futures.successfulAsList(list(tasks, Parals::listen));
	}

	public static <T> ListenableFuture<T> listen(Callable<T> task) {
		try {
			return EXERS.lexor.submit(task);
		} catch (RejectedExecutionException e) {
			logger.error("Rejected");
			throw e;
		}
	}

	public static ListenableFuture<List<Object>> listenRun(Runnable... tasks) {
		return Futures.successfulAsList(list(Arrays.asList(tasks), Parals::listenRun));
	}

	public static ListenableFuture<?> listenRun(Runnable task) {
		try {
			return EXERS.lexor.submit(task);
		} catch (RejectedExecutionException e) {
			logger.error("Rejected");
			throw e;
		}
	}

	public static <V, A, R> R collect(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return collect(Streams.of(col).map(mapper), collector);
	}

	public static <T, T1, K, V> Map<K, V> map(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
		return collect(col.map(mapper), Collectors.toMap(keying, valuing));
	}

	public static <T, K, V> Map<K, V> map(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
		return collect(col, Collectors.toMap(keying, valuing));
	}

	public static <V, A, R> R mapping(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return collect(mapping.apply(Streams.of(col)), collector);
	}

	public static <V, R> R collect(Iterable<? extends V> col, Collector<? super V, ?, R> collector) {
		return collect(Streams.of(col), collector);
	}

	public static <V, R> R collect(Stream<? extends V> s, Collector<? super V, ?, R> collector) {
		if (STREAM_DEBUGGING && logger.isTraceEnabled()) {
			AtomicLong c = new AtomicLong();
			R r = Streams.of(s).peek(e -> c.incrementAndGet()).collect(collector);
			logger.debug("One stream collected [" + c.get() + "] elements, performance issue?");
			return r;
		} else return Streams.of(s).collect(collector);
	}

	public static <V> List<V> list(Stream<? extends V> stream) {
		return collect(Streams.of(stream), Collectors.toList());
	}

	public static <V, R> List<R> list(Stream<V> stream, Function<V, R> mapper) {
		return collect(Streams.of(stream).map(mapper), Collectors.toList());
	}

	public static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return collect(col, mapper, Collectors.toList());
	}

	public static <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		each(Streams.of(col), consumer);
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

	private static <V> void each(Stream<V> s, Consumer<? super V> consumer) {
		get(Futures.successfulAsList(list(Streams.of(s).map(v -> listenRun(() -> consumer.accept(v))))));
	}

	public static String tracePool(String prefix) {
		if (EXERS.exor instanceof ForkJoinPool) {
			ForkJoinPool ex = (ForkJoinPool) EXERS.exor;
			return MessageFormat.format("{5}, Fork/Join: tasks={4}, threads(active/running)={1}/{2}, steals={3}, pool size={0}", ex
					.getPoolSize(), ex.getActiveThreadCount(), ex.getRunningThreadCount(), ex.getStealCount(), ex.getQueuedTaskCount(),
					prefix);
		} else if (EXERS.exor instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor ex = (ThreadPoolExecutor) EXERS.exor;
			return MessageFormat.format("{3}, ThreadPool: tasks={2}, threads(active)={1}, pool size={0}", ex.getPoolSize(), ex
					.getActiveCount(), ex.getTaskCount(), prefix);
		} else return prefix + ": " + EXERS.exor.toString();
	}

	public static int parallelism() {
		if (EXERS.exor instanceof ForkJoinPool) return ((ForkJoinPool) EXERS.exor).getParallelism();
		if (SYS_PARALLELISM < 0) return -SYS_PARALLELISM;
		return Integer.MAX_VALUE;
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

	private final static Exers EXERS = new Exers(EXECUTOR_NAME, SYS_PARALLELISM, false);

	private final static class Exers extends Namedly implements AutoCloseable {
		public static final Logger logger = Logger.getLogger(Exers.class);
		final ExecutorService exor;
		final ListeningExecutorService lexor;
		private final static Map<String, ThreadGroup> g = new ConcurrentHashMap<>();

		public Exers(String name, int parallelism, boolean throwException) {
			UncaughtExceptionHandler handler = (t, e) -> {
				logger.error("Migrater pool task failure @" + t.getName(), e);
				if (throwException) throw wrap(unwrap(e));
			};
			if (parallelism > 0) exor = new ForkJoinPool(parallelism, Concurrents.forkjoinFactory(name), handler, false);
			else {
				ThreadFactory factory = r -> {
					Thread t = new Thread(g.computeIfAbsent(name, n -> new ThreadGroup(name + "ThreadGroup")), r, name + "@" + Texts
							.formatDate(new Date()));
					t.setUncaughtExceptionHandler(handler);
					return t;
				};
				exor = parallelism == 0 ? Executors.newCachedThreadPool(factory) : Executors.newFixedThreadPool(-parallelism, factory);
			}
			if (exor instanceof ThreadPoolExecutor) ((ThreadPoolExecutor) exor).setRejectedExecutionHandler((r, ex) -> logger.error(
					tracePool("Task rejected by the exor")));
			lexor = MoreExecutors.listeningDecorator(exor);
			Systems.handleSignal(sig -> close(), "TERM", "INT");
		}

		@Override
		public void close() {
			logger.debug(name + " is closing...");
		}

		private static int detectParallelism() {
			double f = Double.parseDouble(Configs.gets(PARALLELISM_FACTOR_KEY, "0"));
			if (f <= 0) return (int) f;
			int p = 16 + (int) Math.round((ForkJoinPool.getCommonPoolParallelism() - 16) * (f - 1));
			if (p < 2) p = 2;
			logger.debug("AlbacoreIO parallelism calced as: [" + p + "]\n\t[from: (((-D" + PARALLELISM_FACTOR_KEY + "[" + f
					+ "]) - 1) * (JVM_DEFAULT_PARALLELISM[" + ForkJoinPool.getCommonPoolParallelism()
					+ "] - IO_DEFAULT_PARALLELISM[16])) + IO_DEFAULT_PARALLELISM[16], Max=JVM_DEFAULT_PARALLELISM, Min=2]");
			return p;
		}
	}

	public final static class Pool<V> {
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
}
