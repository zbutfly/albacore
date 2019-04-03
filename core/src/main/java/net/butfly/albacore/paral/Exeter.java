package net.butfly.albacore.paral;

import static net.butfly.albacore.paral.ExeterHandler.DEF_EX;
import static net.butfly.albacore.paral.Task.waitSleep;
import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;

public interface Exeter extends ExecutorService {
	static final Logger logger = Logger.getLogger(Exeter.class);

	int parallelism();

	Future<?> collect(Iterable<Future<?>> futures);

	<T> Future<T> submits(Supplier<T> task);

	<T> List<Future<T>> submit(Collection<? extends Callable<T>> tasks);

	<T> Future<?> submit(Runnable... tasks);

	<T> Future<?> submit(Consumer<T> task, Iterable<T> ins);

	<T> Future<?> submit(T s, Iterable<? extends Consumer<T>> tasks);

	<T> T joins(Supplier<T> task);

	<T> T join(Callable<T> task);

	<T> List<T> join(Collection<? extends Callable<T>> tasks);

	void join(Runnable task);

	void join(Runnable... tasks);

	<T> void join(T in, List<? extends Consumer<T>> tasks);

	<T> void join(Consumer<T> task, Iterable<T> ins);

	class Internal {}

	static Exeter of(ExecutorService ex) {
		if (null == ex) return of();
		if (ex instanceof WrapperExeter) return (WrapperExeter) ex;
		return new WrapperExeter(ex);
	}

	static Exeter of() {
		return DEF_EX;
	}

	static <T> T get(Future<T> future) {
		boolean go = true;
		while (go) try {
			return future.get(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.warn("Task interrupted");
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
		for (Future<?> f : futures) get(f);
	}

	static <T> List<T> get(Iterable<Future<T>> futures) {
		List<T> l = Colls.list();
		for (Future<T> f : futures) {
			T t = get(f);
			if (null != t) l.add(t);
		}
		return l;
	}

	static void get(@SuppressWarnings("rawtypes") Future... futures) {
		if (Objects.requireNonNull(futures).length == 0) return;
		for (Future<?> f : futures) get(f);
	}

	// exectors

	static WrapperExeter newExecutor(String name, int parallelism) {
		return newExecutor(name, parallelism, false);
	}

	static WrapperExeter newExecutor(String name, int parallelism, boolean throwException) {
		ExecutorService exor = null;
		ExeterHandler handler = new ExeterHandler(name, throwException);
		exor = Systems.isDebug() ? handler.debugExec(parallelism) : handler.runExec(parallelism);
		if (null == exor) throw new IllegalArgumentException("Main executor constructed");
		if (exor instanceof ThreadPoolExecutor) //
			((ThreadPoolExecutor) exor).setRejectedExecutionHandler(handler);
		return new WrapperExeter(exor);
	}

	static String tracePool(ExecutorService ex) {
		if (null == ex) return null;
		String c = ex.toString();
		c = c.substring(c.lastIndexOf('.') + 1);
		if (ex instanceof ForkJoinPool) {
			ForkJoinPool x = (ForkJoinPool) ex;
			return c + ": tasks(running/queued)=" + x.getRunningThreadCount() + "/" + x.getQueuedTaskCount() + ", "//
					+ "threads(active/running)=" + x.getActiveThreadCount() + "/" + x.getPoolSize() + ",  stealed=" + x.getStealCount();
		} else if (ex instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor x = (ThreadPoolExecutor) ex;
			return c + ": tasks=" + x.getTaskCount() + ", threads(active)=" + x.getActiveCount() + ", pool size=" + x.getPoolSize();
		} else return ex.toString();
	}

	static ForkJoinWorkerThreadFactory forkjoinFactory(String threadNamePrefix) {
		return null == threadNamePrefix ? ForkJoinPool.defaultForkJoinWorkerThreadFactory : pool -> {
			ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
			if (null != threadNamePrefix) worker.setName(threadNamePrefix + "#" + worker.getPoolIndex());
			return worker;
		};
	}
}
