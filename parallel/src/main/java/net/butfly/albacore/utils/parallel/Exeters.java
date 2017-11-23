package net.butfly.albacore.utils.parallel;

import static net.butfly.albacore.utils.parallel.Exeters.Sleeps.waitSleep;
import static net.butfly.albacore.utils.parallel.Exeters.Throws.unwrap;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.logging.Level;

public interface Exeters {
	final String PROP_PARALLEL_FACTOR = "albacore.parallel.factor";// 1 //"albacore.io.stream.parallelism.factor"
	final String DEF_EXECUTOR_NAME = "AlbacoreIOStream";
	final int DEF_EXECUTOR_PARALLELISM = detectParallelism();
	final Exeter DEFEX = newExecutor(DEF_EXECUTOR_NAME, DEF_EXECUTOR_PARALLELISM);

	static <T> T get(Future<T> f) {
		boolean go = true;
		while (go)
			try {
				return f.get(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				go = false;
			} catch (ExecutionException e) {
				go = false;
				Exeter.logger.log(Level.SEVERE, unwrap(e), () -> "Subtask error");
			} catch (TimeoutException e) {
				waitSleep(10);
				Exeter.logger.finest(() -> "Subtask [" + f.toString() + "] slow....");
			}
		return null;
	}

	static <T> List<T> get(Iterable<Future<T>> futures) {
		List<T> l = new ArrayList<>();
		for (Future<T> f : futures) {
			T t = get(f);
			if (null != t) l.add(get(f));
		}
		return l;
	}

	static void get(Future<?>... futures) {
		for (Future<?> f : futures)
			get(f);
	}

	// exectors
	static int detectParallelism() {
		int fp = ForkJoinPool.getCommonPoolParallelism();
		Exeter.logger.info("ForkJoinPool.getCommonPoolParallelism(): " + fp);
		double f = Double.parseDouble(System.getProperty(PROP_PARALLEL_FACTOR, "1"));
		if (f <= 0) return (int) f;
		int p = 16 + (int) Math.round((fp - 16) * (f - 1));
		if (p < 2) p = 2;
		Exeter.logger.info("AlbacoreIO parallelism adjusted to [" + p + "] by [-D" + PROP_PARALLEL_FACTOR + "=" + f + "].");
		return p;
	}

	static Exeter newExecutor(String name, int parallelism) {
		return newExecutor(name, parallelism, false);
	}

	static Exeter newExecutor(String name, int parallelism, boolean throwException) {
		UncaughtExceptionHandler handler = (t, e) -> {
			Exeter.logger.log(Level.SEVERE, e, () -> "Migrater pool task failure @" + t.getName());
			if (throwException) throw new RuntimeException(e);
		};
		AbstractExecutorService exor = parallelism > 0 ? new ForkJoinPool(parallelism, forkjoinFactory(name), handler, false)
				: newThreadPool(name, parallelism, handler);
		Exeter.logger.info("Main executor constructed: " + exor.toString());
		return new Exeter(exor);
	}

	@Deprecated
	static Exeter newThreadPool(String name, int parallelism, UncaughtExceptionHandler handler) {
		Map<String, ThreadGroup> g = new ConcurrentHashMap<>();
		ThreadFactory factory = r -> {
			Thread t = new Thread(g.computeIfAbsent(name, n -> new ThreadGroup(name + "ThreadGroup")), r, name + "@[" + new Date()
					.toString() + "]");
			t.setUncaughtExceptionHandler(handler);
			return t;
		};
		RejectedExecutionHandler rejected = (r, exec) -> Exeter.logger.severe(() -> "Paral task rejected by [" + exec.toString() + "]");
		ThreadPoolExecutor tp = parallelism == 0 ? //
				new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), factory, rejected) : //
				new ThreadPoolExecutor(-parallelism, -parallelism, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), factory,
						rejected);
		tp.setRejectedExecutionHandler((r, ex) -> Exeter.logger.severe(() -> tracePool(tp, "Task rejected")));
		return new Exeter(tp);
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

	interface Sleeps {
		final long DEF_WAIT_MS = 100;

		static boolean waitSleep(Supplier<Boolean> waiting) {
			while (waiting.get())
				if (!waitSleep()) return false;
			return true;
		}

		static boolean waitSleep() {
			return waitSleep(DEF_WAIT_MS);
		}

		static boolean waitSleep(long millis) {
			if (millis < 0) return true;
			try {
				Thread.sleep(millis);
				return true;
			} catch (InterruptedException e) {
				return false;
			}
		}
	}

	class Throws {
		private static final ReentrantReadWriteLock METHODS_LOCK = new ReentrantReadWriteLock();
		private static final Map<Class<? extends Throwable>, Method> WRAPPING_METHODS = initWrappingMethods();

		private static Map<Class<? extends Throwable>, Method> initWrappingMethods() {
			try {
				Map<Class<? extends Throwable>, Method> m = new ConcurrentHashMap<>();
				m.put(ExecutionException.class, ExecutionException.class.getMethod("getCause"));
				m.put(InvocationTargetException.class, InvocationTargetException.class.getMethod("getTargetException"));
				m.put(UndeclaredThrowableException.class, UndeclaredThrowableException.class.getMethod("getUndeclaredThrowable"));
				m.put(RuntimeException.class, RuntimeException.class.getMethod("getCause"));
				return m;
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}

		public static void unwrap(Class<? extends Throwable> t, String methodName) {
			METHODS_LOCK.writeLock().lock();
			try {
				WRAPPING_METHODS.put(t, t.getMethod(methodName));
			} catch (NoSuchMethodException e) {
				throw new RuntimeException();
			} finally {
				METHODS_LOCK.writeLock().unlock();
			}
		}

		public static Throwable unwrap(Throwable ex) {
			if (null == ex) return null;
			METHODS_LOCK.readLock().lock();
			try {
				for (Entry<Class<? extends Throwable>, Method> t : WRAPPING_METHODS.entrySet())
					if (t.getKey().isAssignableFrom(ex.getClass())) try {
						Throwable cause = (Throwable) t.getValue().invoke(ex);
						return null == cause || ex.equals(cause) ? ex : unwrap(cause);
					} catch (Exception e) {}
			} finally {
				METHODS_LOCK.readLock().unlock();
			}
			return ex;
		}

	}
}
