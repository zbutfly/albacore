package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.lambda.Callable;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Concurrents extends Utils {
	private static final Logger logger = Logger.getLogger(Concurrents.class);
	static ExecutorService CORE_EXECUTOR = executor(null);

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
	public static ExecutorService executor(String key) {
		final int c = Integer.parseInt(System.getProperty("albacore.concurrence", "0"));
		logger.info(() -> {
			StringBuilder sb = new StringBuilder("Albacore task concurrence configuration ").append(c < -1 ? "negitive" : "").append(" ("
					+ c + "), use ");
			if (c < -1) sb.append("fixed size (").append(-c).append(") thread pool.");
			else if (c == -1) sb.append("inlimited cached thread pool.");
			else if (c == 0) sb.append("fork join (work stealing) thread pool with AUTO parallelism.");
			else sb.append("fork join (work stealing) thread pool with ").append(-c).append(" parallelism.");
			return sb;
		});
		return executor(key, c);
	}

	/**
	 * @param key
	 *            Create or fetch {@link ExecutorService} with key based on
	 *            {@link Concurrents#executor(int)} with concurrence by System
	 *            Property "albacore.concurrence", default 0.
	 * @return
	 */
	public static ExecutorService executor(String key, int concurrence) {
		return Instances.fetch(() -> {
			return executor(concurrence);
		}, ExecutorService.class, key);
	}

	/**
	 * @param concurrence
	 *            <br>
	 *            -N: {@link Executors#newFixedThreadPool} with size N <br>
	 *            -1: {@link Executors#newCachedThreadPool} <br>
	 *            0: {@link ForkJoinPool} with SYSTEM parallelism <br>
	 *            N: {@link ForkJoinPool} with parallelism N
	 */
	public static ExecutorService executor(int concurrence) {
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
			return Executors.newWorkStealingPool();
		} else {
			logger.info("Albacore task concurrence configuration negative (" + concurrence
					+ "), use fork join (work stealing) thread pool with " + -concurrence + " parallelism.");
			return Executors.newWorkStealingPool(concurrence);
		}
	}
}
