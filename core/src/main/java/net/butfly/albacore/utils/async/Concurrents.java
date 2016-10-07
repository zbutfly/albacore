package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;

import net.butfly.albacore.lambda.Callable;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Concurrents extends Utils {
	private static final Logger logger = Logger.getLogger(Concurrents.class);
	static ExecutorService CORE_EXECUTOR = executor();

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
	public static ExecutorService executor(String... key) {
		final int c = Integer.parseInt(System.getProperty("albacore.concurrence", "0"));
		return executor(c, key);
	}

	/**
	 * @param key
	 *            Create or fetch {@link ExecutorService} with key based on
	 *            {@link Concurrents#executor(int)} with concurrence by System
	 *            Property "albacore.concurrence", default 0.
	 * @return
	 */
	public static ExecutorService executor(int concurrence, String... key) {
		String[] keys = null == key || key.length == 0 ? new String[] { "" } : key;
		return Instances.fetch(() -> {
			final ExecutorService e = executor(concurrence);
			logger.info("ExecutorService [" + e.getClass() + "] with key: [" + Joiner.on('.').join(keys) + "] created.");
			return e;
		}, ExecutorService.class, (Object[]) keys);
	}

	/**
	 * @param concurrence
	 *            <br>
	 *            -N: {@link Executors#newFixedThreadPool} with size N <br>
	 *            -1: {@link Executors#newCachedThreadPool} <br>
	 *            0: {@link ForkJoinPool} with SYSTEM parallelism <br>
	 *            N: {@link ForkJoinPool} with parallelism N
	 */
	private static ExecutorService executor(int concurrence) {
		if (concurrence < -1) {
			logger.info("Albacore task concurrence configuration (" + concurrence + "), use fixed size thread pool.");
			if (concurrence < 5) logger.warn("Albacore task concurrence configuration too small (" + concurrence + "), debugging? ");
			return Executors.newFixedThreadPool(-concurrence);
		} else if (concurrence == -1) {
			logger.info("Albacore task concurrence configuration (" + concurrence + "), use inlimited cached thread pool.");
			return Executors.newCachedThreadPool();
		} else if (concurrence == 0) {
			logger.info("Albacore task concurrence configuration (" + concurrence
					+ "), use fork join (work stealing) thread pool with AUTO parallelism.");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newCachedThreadPool();
			}
			return Executors.newWorkStealingPool();
		} else {
			logger.info("Albacore task concurrence configuration negative (" + concurrence
					+ "), use fork join (work stealing) thread pool with " + -concurrence + " parallelism.");
			if (Systems.isDebug()) {
				logger.warn("Debug mode, use fix pool to enable breakpoints.");
				return Executors.newFixedThreadPool(concurrence);
			}
			return Executors.newWorkStealingPool(concurrence);
		}
	}
}
