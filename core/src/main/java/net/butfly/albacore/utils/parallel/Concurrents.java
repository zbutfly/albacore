package net.butfly.albacore.utils.parallel;

import static net.butfly.albacore.paral.Task.DEF_WAIT_MS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

@Deprecated
public final class Concurrents extends Utils {
	private static final Logger logger = Logger.getLogger(Concurrents.class);
	private static ListeningExecutorService CORE_EXECUTOR = null;

	public static List<Object> submitAndWait(ListeningExecutorService ex, Supplier<Runnable> tasking, int parallelism) {
		List<ListenableFuture<?>> outs = new ArrayList<>();
		for (int i = 0; i < parallelism; i++)
			outs.add(ex.submit(tasking.get()));
		try {
			return Futures.successfulAsList(outs).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException("Task failure", Exceptions.unwrap(e));
		} finally {
			ex.shutdown();
			Concurrents.waitShutdown(ex, logger);
		}
	}

	public static void waitFull(ExecutorService executor, Logger logger) {
		ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
		while (pool.getActiveCount() >= pool.getMaximumPoolSize()) {
			logger.trace("Executor [" + pool.getClass().getSimpleName() + "] is full, waiting...");
			waitSleep();
		}
	}

	public static boolean shutdown() {
		CORE_EXECUTOR.shutdown();
		waitShutdown();
		return true;
	}

	public static boolean waitShutdown(ExecutorService executor, long seconds, Logger logger) {
		while (true)
			try {
				logger.trace("Waiting for exor terminate...");
				if (executor.awaitTermination(seconds, TimeUnit.SECONDS)) return true;
			} catch (InterruptedException e) {
				logger.warn("Not all processing thread finished correctly, waiting interrupted.");
				return false;
			}
	}

	public static boolean waitShutdown(ExecutorService executor, Logger logger) {
		return waitShutdown(executor, DEF_WAIT_MS * 10, logger);
	}

	public static boolean waitShutdown() {
		return waitShutdown(CORE_EXECUTOR, logger);
	}

	public static Future<?> submit(Runnable thread) {
		return CORE_EXECUTOR.submit(thread);
	}

	public static <V> Future<V> submit(Callable<V> thread) {
		return CORE_EXECUTOR.submit(thread);
	}

	public static boolean shutdown(ExecutorService executor) {
		return shutdown(executor, true);
	}

	public static boolean shutdown(ExecutorService executor, boolean wait) {
		if (wait) {
			executor.shutdown();
			while (true)
				try {
					if (executor.awaitTermination(DEF_WAIT_MS, TimeUnit.MILLISECONDS)) return true;
				} catch (InterruptedException e) {
					return false;
				}
		} else {
			executor.shutdownNow();
			return true;
		}
	}

	/**
	 * @param key
	 *            Create or fetch {@link ExecutorService} with key based on {@link Concurrents#executor(int)}, with concurrence defined by
	 *            System Property Albacore.Props.PROP_CURRENCE, default 0.
	 * @return
	 */
	public static ListeningExecutorService executor(String... name) {
		return executor(Integer.parseInt(System.getProperty(Albacore.Props.PROP_CURRENCE, "0")), name);
	}

	/**
	 * @param key
	 *            Create or fetch {@link ExecutorService} with key.<br>
	 *            Concurrence by System Property Albacore.Props.PROP_CURRENCE, default 0.<br>
	 *            Forkjoin first by System Property Albacore.Props.PROP_CURRENCE_FORKJOIN, default true.
	 * @param concurrence
	 *            <br>
	 *            -N: {@link Executors#newFixedThreadPool} with size N <br>
	 *            -1: {@link Executors#newCachedThreadPool} <br>
	 *            0: {@link ForkJoinPool} with SYSTEM parallelism <br>
	 *            N: {@link ForkJoinPool} with parallelism N
	 */
	public static ListeningExecutorService executor(int concurrence, String... name) {
		boolean forkjoin = Boolean.parseBoolean(System.getProperty(Albacore.Props.PROP_CURRENCE_FORKJOIN, "true"));
		logger.info("ForkJoin first? " + forkjoin + "!, change it by -Dalbacore.concurrent.forkjoin=false");
		return Instances.fetch(() -> {
			String prefix = name == null || name.length == 0 ? "AlbacoreThread" : Joiner.on('-').join(name);
			ListeningExecutorService e = executor(concurrence, new ThreadFactoryBuilder().setNameFormat(prefix + "-%d").build());
			logger.info("ExecutorService [" + e.getClass() + "] with name: [" + prefix + "] created.");
			return e;
		}, ListeningExecutorService.class, (Object[]) name);
	}

	public static ListeningExecutorService executor() {
		if (CORE_EXECUTOR == null) CORE_EXECUTOR = executor(0);
		return CORE_EXECUTOR;
	}

	public static ListeningExecutorService executor(int c, ThreadFactory fac) {
		boolean forkjoin = Boolean.parseBoolean(System.getProperty(Albacore.Props.PROP_CURRENCE_FORKJOIN, "true"));
		return MoreExecutors.listeningDecorator(forkjoin ? forkjoin(c, fac) : classic(c, fac));
	}

	private static ExecutorService forkjoin(int c, ThreadFactory fac) {
		if (c < -1) {
			logger.info("Albacore creating FixedThreadPool (parallelism:" + -c + ").");
			if (c >= -5) logger.warn("Albacore task concurrence configuration too small (" + -c + "), debugging? ");
			return Executors.newFixedThreadPool(-c, fac);
		} else if (c == -1) {
			logger.info("Albacore creating infinite CachedThreadPool on concurrence (" + c + ").");
			return Executors.newCachedThreadPool(fac);
		} else if (c == 0) {
			logger.info("Albacore creating ForkJoin(WorkStealing) ThreadPool (parallelism: default(JVM)).");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newCachedThreadPool(fac);
			} else return Executors.newWorkStealingPool();
		} else {
			logger.info("Albacore creating ForkJoin(WorkStealing)ThreadPool (parallelism:" + c + ").");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newFixedThreadPool(c, fac);
			} else return Executors.newWorkStealingPool(c);
		}
	}

	private static ExecutorService classic(int c, ThreadFactory fac) {
		if (c < -1) {
			logger.info("Albacore creating ForkJoin(WorkStealing)ThreadPool (parallelism:" + -c + ").");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newFixedThreadPool(-c, fac);
			} else return Executors.newWorkStealingPool(-c);
		} else if (c == -1) {
			logger.info("Albacore creating ForkJoin(WorkStealing) ThreadPool (parallelism: default(JVM)).");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newCachedThreadPool(fac);
			} else return Executors.newWorkStealingPool();
		} else if (c == 0) {
			logger.info("Albacore creating infinite CachedThreadPool on concurrence (" + c + ").");
			return Executors.newCachedThreadPool(fac);
		} else {
			logger.info("Albacore creating FixedThreadPool (parallelism:" + c + ").");
			if (c < 5) logger.warn("Albacore task concurrence configuration too small (" + c + "), debugging? ");
			return Executors.newFixedThreadPool(c, fac);
		}
	}

	public static <T> List<T> successfulList(List<? extends java.util.concurrent.Callable<T>> tasks, int parallelism) {
		ListeningExecutorService ex = Concurrents.executor(parallelism);
		List<ListenableFuture<T>> fl = new ArrayList<>();
		for (java.util.concurrent.Callable<T> t : tasks)
			fl.add(ex.submit(t));
		try {
			return Futures.successfulAsList(fl).get();
		} catch (InterruptedException e) {
			logger.error("Concurrent interrupted", e);
			return new ArrayList<>();
		} catch (ExecutionException e) {
			logger.error("Concurrent failure (maybe partly)", e.getCause());
			throw new RuntimeException(e.getCause());
		}
	}

	public static <T> List<T> successfulList(List<? extends java.util.concurrent.Callable<T>> tasks, long timeout, TimeUnit u) {
		ListeningExecutorService ex = Concurrents.executor();
		List<ListenableFuture<T>> fl = new ArrayList<>();
		for (java.util.concurrent.Callable<T> t : tasks)
			fl.add(ex.submit(t));
		try {
			return Futures.successfulAsList(fl).get(timeout, u);
		} catch (InterruptedException e) {
			logger.error("Concurrent interrupted", e);
			return new ArrayList<>();
		} catch (ExecutionException e) {
			logger.error("Concurrent failure (maybe partly)", e.getCause());
			throw new RuntimeException(e.getCause());
		} catch (TimeoutException e) {
			logger.error("Concurrent timeout (maybe partly)");
			throw new RuntimeException(e.getCause());
		}
	}

	public static <T> List<T> successfulList(List<? extends java.util.concurrent.Callable<T>> tasks, int parallelism, long timeout,
			TimeUnit u) {
		ListeningExecutorService ex = Concurrents.executor(parallelism);
		List<ListenableFuture<T>> fl = new ArrayList<>();
		for (java.util.concurrent.Callable<T> t : tasks)
			fl.add(ex.submit(t));
		try {
			return Futures.successfulAsList(fl).get(timeout, u);
		} catch (InterruptedException e) {
			logger.error("Concurrent interrupted", e);
			return new ArrayList<>();
		} catch (ExecutionException e) {
			logger.error("Concurrent failure (maybe partly)", e.getCause());
			throw new RuntimeException(e.getCause());
		} catch (TimeoutException e) {
			logger.error("Concurrent timeout (maybe partly)");
			throw new RuntimeException(e.getCause());
		}
	}

	@Deprecated
	public static boolean waitSleep() {
		return waitSleep();
	}

	@Deprecated
	public static boolean waitSleep(long mills) {
		return waitSleep(mills);
	}

	@Deprecated
	public static boolean waitSleep(Supplier<Boolean> waiting) {
		return waitSleep(waiting);
	}
}
