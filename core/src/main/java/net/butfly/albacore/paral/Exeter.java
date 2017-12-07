package net.butfly.albacore.paral;

import static net.butfly.albacore.Albacore.Props.PROP_PARALLEL_FACTOR;
import static net.butfly.albacore.paral.Task.waitSleep;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.lang.Thread.UncaughtExceptionHandler;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public interface Exeter extends ExecutorService {
	static final Logger logger = Logger.getLogger(Exeter.class);
	final static Exeter synchro = new CurrentExeter();

	int parallelism();

	Future<?> collect(Iterable<Future<?>> futures);

	<T> Future<T> submit(Supplier<T> task);

	<T> List<Future<T>> submit(Collection<? extends Callable<T>> tasks);

	<T> Future<?> submit(Runnable... tasks);

	<T> T join(Supplier<T> task);

	<T> T join(Callable<T> task);

	<T> List<T> join(Collection<? extends Callable<T>> tasks);

	void join(Runnable... tasks);

	void join(Runnable task);

	class Internal {
		private static final String DEF_EXECUTOR_NAME = "AlbacoreIOStream";
		static final int DEF_EXECUTOR_PARALLELISM = detectParallelism();
		static final WrapperExeter DEFEX = newExecutor(DEF_EXECUTOR_NAME, DEF_EXECUTOR_PARALLELISM);
	}

	static Exeter of(ExecutorService ex) {
		if (null == ex) return of();
		if (ex instanceof WrapperExeter) return (WrapperExeter) ex;
		return new WrapperExeter(ex);
	}

	static Exeter of() {
		return Internal.DEFEX;
	}

	static <T> T get(Future<T> future) {
		boolean go = true;
		while (go)
			try {
				return future.get(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				go = false;
			} catch (ExecutionException e) {
				go = false;
				logger.error("Subtask error", unwrap(e));
			} catch (TimeoutException e) {
				waitSleep(10);
				logger.trace(() -> "Subtask [" + future.toString() + "] slow....");
			}
		return null;
	}

	static void getn(Iterable<Future<?>> futures) {
		for (Future<?> f : futures)
			get(f);
	}

	static <T> List<T> get(Iterable<Future<T>> futures) {
		List<T> l = Colls.list();
		for (Future<T> f : futures) {
			T t = get(f);
			if (null != t) l.add(get(f));
		}
		return l;
	}

	static void get(@SuppressWarnings("rawtypes") Future... futures) {
		if (Objects.requireNonNull(futures).length == 0) return;
		for (Future<?> f : futures)
			get(f);
	}

	// exectors
	static int detectParallelism() {
		int fp = ForkJoinPool.getCommonPoolParallelism();
		logger.info("ForkJoinPool.getCommonPoolParallelism(): " + fp);
		double f = Double.parseDouble(System.getProperty(PROP_PARALLEL_FACTOR, "1"));
		if (f <= 0) return (int) f;
		int p = 16 + (int) Math.round((fp - 16) * (f - 1));
		if (p < 2) p = 2;
		logger.info("AlbacoreIO parallelism adjusted to [" + p + "] by [-D" + PROP_PARALLEL_FACTOR + "=" + f + "].");
		return p;
	}

	static WrapperExeter newExecutor(String name, int parallelism) {
		return newExecutor(name, parallelism, false);
	}

	static WrapperExeter newExecutor(String name, int parallelism, boolean throwException) {
		UncaughtExceptionHandler handler = (t, e) -> {
			logger.error("Migrater pool task failure @" + t.getName(), e);
			if (throwException) throw new RuntimeException(e);
		};
		ExecutorService exor = parallelism > 0 ? new ForkJoinPool(parallelism, forkjoinFactory(name), handler, true)
				: newThreadPool(name, parallelism, handler);
		logger.info("Main executor constructed: " + exor.toString());
		return new WrapperExeter(exor);
	}

	@Deprecated
	static WrapperExeter newThreadPool(String name, int parallelism, UncaughtExceptionHandler handler) {
		Map<String, ThreadGroup> g = Maps.of();
		ThreadFactory factory = r -> {
			Thread t = new Thread(g.computeIfAbsent(name, n -> new ThreadGroup(name + "ThreadGroup")), r, name + "@[" + new Date()
					.toString() + "]");
			t.setUncaughtExceptionHandler(handler);
			return t;
		};
		RejectedExecutionHandler rejected = (r, exec) -> logger.error("Paral task rejected by [" + exec.toString() + "]");
		ThreadPoolExecutor tp = parallelism == 0 ? //
				new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory, rejected) : //
				new ThreadPoolExecutor(-parallelism, -parallelism, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), factory,
						rejected);
		tp.setRejectedExecutionHandler((r, ex) -> logger.error(tracePool(tp, "Task rejected")));
		return new WrapperExeter(tp);
	}

	static String tracePool(ExecutorService exec, String prefix) {
		if (exec instanceof ForkJoinPool) {
			ForkJoinPool ex = (ForkJoinPool) exec;
			return MessageFormat.format("{5}, Fork/Join: tasks={4}, threads(active/running)={1}/{2}, steals={3}, pool size={0}", ex
					.getPoolSize(), ex.getActiveThreadCount(), ex.getRunningThreadCount(), ex.getStealCount(), ex.getQueuedTaskCount(),
					prefix);
		} else if (exec instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor ex = (ThreadPoolExecutor) exec;
			return MessageFormat.format("{3}, ThreadPool: tasks={2}, threads(active)={1}, pool size={0}", ex.getPoolSize(), ex
					.getActiveCount(), ex.getTaskCount(), prefix);
		} else return prefix + ": " + exec.toString();
	}

	static ForkJoinWorkerThreadFactory forkjoinFactory(String threadNamePrefix) {
		return null == threadNamePrefix ? ForkJoinPool.defaultForkJoinWorkerThreadFactory : pool -> {
			ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
			if (null != threadNamePrefix) worker.setName(threadNamePrefix + "#" + worker.getPoolIndex());
			return worker;
		};
	}

}
