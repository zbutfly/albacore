package net.butfly.albacore.utils.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.lambda.Callable;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Concurrents extends Utils {
	private static final Logger logger = Logger.getLogger(Concurrents.class);
	private static ListeningExecutorService CORE_EXECUTOR = null;

	public static List<Object> submitAndWait(ListeningExecutorService ex, Supplier<Runnable> tasking, int parallelism) {
		List<ListenableFuture<?>> outs = new ArrayList<>();
		for (int i = 0; i < parallelism; i++)
			outs.add(ex.submit(tasking.get()));
		try {
			return Futures.allAsList(outs).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException("Task failure", e);
		} finally {
			ex.shutdown();
			Concurrents.waitShutdown(ex, logger);
		}
	}

	public static void waitFull(ExecutorService executor, Logger logger) {
		ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
		while (pool.getActiveCount() >= pool.getMaximumPoolSize()) {
			logger.trace("Executor [" + pool.getClass().getSimpleName() + "] is full, waiting...");
			waitSleep(1000);
		}
	}

	public static boolean shutdown() {
		CORE_EXECUTOR.shutdown();
		waitShutdown();
		return true;
	}

	public static boolean waitShutdown(ExecutorService executor, long seconds, Logger logger) {
		boolean running = true;
		while (running)
			try {
				logger.trace("Waiting for executor terminate...");
				running = executor.awaitTermination(seconds, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.warn("Not all processing thread finished correctly, waiting interrupted.");
			}
		return true;
	}

	public static boolean waitShutdown(ExecutorService executor, Logger logger) {
		return waitShutdown(executor, 10, logger);
	}

	public static boolean waitSleep(long millis) {
		return waitSleep(millis, null, null);
	}

	public static boolean waitSleep(long millis, Logger logger, CharSequence cause) {
		try {
			if (null != logger && logger.isTraceEnabled()) logger.trace("Thread [" + Thread.currentThread().getName() + "] sleep for ["
					+ millis + "ms], cause [" + cause + "].");
			Thread.sleep(millis);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
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

	/**
	 * @param key
	 *            Create or fetch {@link ExecutorService} with key based on
	 *            {@link Concurrents#executor(int)}, with concurrence defined by
	 *            System Property "albacore.concurrence", default 0.
	 * @return
	 */
	public static ListeningExecutorService executor(String... name) {
		return executor(Integer.parseInt(System.getProperty("albacore.concurrence", "0")), name);
	}

	/**
	 * @param key
	 *            Create or fetch {@link ExecutorService} with key.<br>
	 *            Concurrence by System Property "albacore.concurrence", default
	 *            0.<br>
	 *            Forkjoin first by System Property
	 *            "albacore.concurrent.forkjoin", default true.
	 * @param concurrence
	 *            <br>
	 *            -N: {@link Executors#newFixedThreadPool} with size N <br>
	 *            -1: {@link Executors#newCachedThreadPool} <br>
	 *            0: {@link ForkJoinPool} with SYSTEM parallelism <br>
	 *            N: {@link ForkJoinPool} with parallelism N
	 */
	public static ListeningExecutorService executor(int concurrence, String... name) {
		boolean forkjoin = Boolean.parseBoolean(System.getProperty("albacore.concurrent.forkjoin", "true"));
		logger.info("ForkJoin first? " + forkjoin + "!, change it by -Dalbacore.concurrent.forkjoin=false");
		return Instances.fetch(() -> {
			ListeningExecutorService e = executor(concurrence);
			logger.info("ExecutorService [" + e.getClass() + "] with name: [" + name + "] created.");
			return e;
		}, ListeningExecutorService.class, (Object[]) name);
	}

	public static ListeningExecutorService executor() {
		if (CORE_EXECUTOR == null) CORE_EXECUTOR = executor(0);
		return CORE_EXECUTOR;
	}

	private static ListeningExecutorService executor(int c) {
		boolean forkjoin = Boolean.parseBoolean(System.getProperty("albacore.concurrent.forkjoin", "true"));
		return MoreExecutors.listeningDecorator(forkjoin ? forkjoin(c) : classic(c));
	}

	private static ExecutorService forkjoin(int c) {
		if (c < -1) {
			logger.info("Albacore creating FixedThreadPool (parallelism:" + -c + ").");
			if (c >= -5) logger.warn("Albacore task concurrence configuration too small (" + -c + "), debugging? ");
			return Executors.newFixedThreadPool(-c);
		} else if (c == -1) {
			logger.info("Albacore creating infinite CachedThreadPool on concurrence (" + c + ").");
			return Executors.newCachedThreadPool();
		} else if (c == 0) {
			logger.info("Albacore creating ForkJoin(WorkStealing) ThreadPool (parallelism: default(JVM)).");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newCachedThreadPool();
			} else return Executors.newWorkStealingPool();
		} else {
			logger.info("Albacore creating ForkJoin(WorkStealing)ThreadPool (parallelism:" + c + ").");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newFixedThreadPool(c);
			} else return Executors.newWorkStealingPool(c);
		}
	}

	private static ExecutorService classic(int c) {
		if (c < -1) {
			logger.info("Albacore creating ForkJoin(WorkStealing)ThreadPool (parallelism:" + -c + ").");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newFixedThreadPool(-c);
			} else return Executors.newWorkStealingPool(-c);
		} else if (c == -1) {
			logger.info("Albacore creating ForkJoin(WorkStealing) ThreadPool (parallelism: default(JVM)).");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newCachedThreadPool();
			} else return Executors.newWorkStealingPool();
		} else if (c == 0) {
			logger.info("Albacore creating infinite CachedThreadPool on concurrence (" + c + ").");
			return Executors.newCachedThreadPool();
		} else {
			logger.info("Albacore creating FixedThreadPool (parallelism:" + c + ").");
			if (c < 5) logger.warn("Albacore task concurrence configuration too small (" + c + "), debugging? ");
			return Executors.newFixedThreadPool(c);
		}
	}

	public static Runnable forever(Runnable r, Runnable... then) {
		return () -> {
			while (true) {
				r.run();
				for (Runnable t : then)
					t.run();
			}
		};
	}

	public static Runnable untile(Supplier<Boolean> stopping, Runnable r, Runnable... then) {
		return () -> {
			while (!stopping.get()) {
				r.run();
				for (Runnable t : then)
					t.run();
			}
		};
	}
}
