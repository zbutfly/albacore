package net.butfly.albacore.utils.parallel;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;
import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.lang.Thread.UncaughtExceptionHandler;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;
import static net.butfly.albacore.utils.parallel.Lambdas.func;

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
	private final static int SYS_PARALLELISM = Exers.detectParallelism();
	private final static Exers EXERS = new Exers(EXECUTOR_NAME, SYS_PARALLELISM, false);

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

	public static void run(Runnable task) {
		get(EXERS.exor.submit(task));
	}

	/**
	 * Run parallelly
	 * 
	 * @param tasks
	 */
	public static void run(Runnable... tasks) {
		List<Future<?>> fs = new ArrayList<>();
		for (Runnable t : tasks)
			fs.add(EXERS.exor.submit(t));
		for (Future<?> f : fs)
			get(f);
	}

	// /**
	// * Run sequentially, by listening one by one
	// *
	// * @param firstAndThens
	// */
	// @SuppressWarnings({ "rawtypes", "unchecked" })
	// @Deprecated
	// public static void runs(Runnable... firstAndThens) {
	// if (null == firstAndThens || firstAndThens.length == 0) return;
	// ListenableFuture f = EXERS.lexor.submit(firstAndThens[0]);
	// if (firstAndThens.length > 1) f.addListener(() ->
	// runs(Arrays.copyOfRange(firstAndThens, 1, firstAndThens.length)),
	// EXERS.exor);
	// get(f);
	// }
	//
	// @SafeVarargs
	// @Deprecated
	// public static <T> void runs(Callable<T> first, Consumer<T>... then) {
	// if (null == first) return;
	// ListenableFuture<T> f = EXERS.lexor.submit(first);
	// for (Consumer<T> t : then)
	// if (t != null) f.addListener(() -> {
	// try {
	// t.accept(f.get());
	// } catch (InterruptedException e) {} catch (ExecutionException e) {
	// logger.error("Subtask error", unwrap(e));
	// }
	// }, EXERS.exor);
	// get(f);
	// }

	public static <T> T run(Callable<T> task) {
		return get(EXERS.exor.submit(task));
	}

	@SafeVarargs
	public static <T> List<T> run(Callable<T>... tasks) {
		return run(Arrays.asList(tasks));
	}

	public static <T> List<T> run(List<Callable<T>> tasks) {
		List<Future<T>> fs = new ArrayList<>();
		for (Callable<T> t : tasks)
			fs.add(EXERS.exor.submit(t));
		List<T> rs = new CopyOnWriteArrayList<>();
		for (Future<T> f : fs)
			rs.add(get(f));
		return rs;
	}

	public static <T> Future<List<T>> listen(List<Callable<T>> tasks) {
		return listen(() -> run(tasks));
	}

	public static <T> Future<T> listen(Callable<T> task) {
		try {
			return EXERS.exor.submit(task);
		} catch (RejectedExecutionException e) {
			logger.error("Rejected");
			throw e;
		}
	}

	public static Future<?> listen(Runnable... tasks) {
		return listen(() -> run(tasks));
	}

	public static Future<?> listen(Runnable task) {
		try {
			return EXERS.exor.submit(task);
		} catch (RejectedExecutionException e) {
			logger.error("Rejected");
			throw e;
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
		return map(src, v -> run(() -> doing.apply(v)), Collectors.reducing(null, accumulator));
	}

	/**
	 * Strict Parallel traversing.
	 * 
	 * @param src
	 * @param doing
	 * @return couting
	 */
	public static <V> long eachs(Iterable<V> src, Consumer<V> doing) {
		return map(src, v -> run(() -> {
			doing.accept(v);
			return null;
		}), Collectors.counting());
	}

	public static <V> long eachs(Stream<V> src, Consumer<V> doing) {
		return map(src, v -> run(() -> {
			doing.accept(v);
			return null;
		}), Collectors.counting());
	}

	public static <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		each(of(col), consumer);
	}

	private static <V> void each(Stream<V> s, Consumer<? super V> consumer) {
		run(() -> map(s, v -> get(listen(() -> consumer.accept((V) v))), Collectors.counting()));
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

	public static <T> List<T> get(List<Future<T>> fs) {
		return list(fs, Parals::get);
	}

	public static <T> T get(Future<T> f) {
		boolean go = true;
		while (go)
			try {
				return f.get(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				go = false;
			} catch (ExecutionException e) {
				go = false;
				logger.error("Subtask error", unwrap(e));
			} catch (TimeoutException e) {
				Concurrents.waitSleep(10);
				logger.trace(() -> "Subtask [" + f.toString() + "] slow....");
			}
		return null;
	}

	private final static class Exers extends Namedly implements AutoCloseable {
		final ExecutorService exor;

		public Exers(String name, int parallelism, boolean throwException) {
			UncaughtExceptionHandler handler = (t, e) -> {
				logger.error("Migrater pool task failure @" + t.getName(), e);
				if (throwException) throw wrap(unwrap(e));
			};
			exor = parallelism > 0 ? new ForkJoinPool(parallelism, Concurrents.forkjoinFactory(name), handler, false)
					: threadPool(parallelism, handler);
			logger.info("Main executor constructed: " + exor.toString());
			Systems.handleSignal(sig -> close(), "TERM", "INT");
		}

		private ThreadPoolExecutor threadPool(int parallelism, UncaughtExceptionHandler handler) {
			Map<String, ThreadGroup> g = new ConcurrentHashMap<>();
			ThreadFactory factory = r -> {
				Thread t = new Thread(g.computeIfAbsent(name, n -> new ThreadGroup(name + "ThreadGroup")), r, name + "@" + Texts.formatDate(
						new Date()));
				t.setUncaughtExceptionHandler(handler);
				return t;
			};
			RejectedExecutionHandler rejected = (r, ex) -> logger.error("Paral task rejected", ex);
			ThreadPoolExecutor tp = parallelism == 0 ? //
					new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory, rejected)
					: //
					new ThreadPoolExecutor(-parallelism, -parallelism, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
							factory, rejected);
			tp.setRejectedExecutionHandler((r, ex) -> logger.error(tracePool("Task rejected by the exor")));
			return tp;
		}

		@Override
		public void close() {
			logger.debug(name + " is closing...");
		}

		private static int detectParallelism() {
			int fp = ForkJoinPool.getCommonPoolParallelism();
			logger.info("ForkJoinPool.getCommonPoolParallelism(): " + fp);
			double f = Double.parseDouble(Configs.gets(Albacore.Props.PROP_PARALLEL_FACTOR, "1"));
			if (f <= 0) return (int) f;
			int p = 16 + (int) Math.round((fp - 16) * (f - 1));
			if (p < 2) p = 2;
			logger.info("AlbacoreIO parallelism adjusted to [" + p + "] by [-D" + Albacore.Props.PROP_PARALLEL_FACTOR + "=" + f + "].");
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

	public static String status() {
		return status(EXERS.exor);
	}

	public static String status(ExecutorService exor) {
		if (null == exor) return null;
		if (exor instanceof ThreadPoolExecutor || exor instanceof ForkJoinPool) return exor.toString();
		Object o = Reflections.get(exor, "e");// DelegatedExecutorService
		if (null == o) return null;
		return o instanceof ExecutorService ? status((ExecutorService) o) : exor.toString();
	}

}
