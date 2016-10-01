package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.lambda.Callable;
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

	private static ExecutorService executor() {
		int c;
		try {
			c = Integer.parseInt(System.getProperty("albacore.tasks.concurrence"));
		} catch (Exception ex) {
			c = 0;
		}
		if (c < -1) {
			logger.warn("Albacore task concurrence configuration negative (" + c + "), use work stealing thread pool with " + -c
					+ " parallelism.");
			return Executors.newWorkStealingPool(-c);
		} else if (c == -1) {
			logger.warn("Albacore task concurrence configuration negative (-1), use work stealing thread pool with AUTO parallelism.");
			return Executors.newWorkStealingPool(-c);
		} else if (c == 0) {
			logger.info("Albacore task concurrence configuration (" + c + "), use inlimited cached thread pool.");
			return CORE_EXECUTOR = Executors.newCachedThreadPool();
		} else {
			logger.info("Albacore task concurrence configuration (" + c + "), use fixed size thread pool.");
			if (c < 5) logger.warn("Albacore task concurrence configuration too small (" + c + "), debugging? ");
			return CORE_EXECUTOR = Executors.newFixedThreadPool(c);
		}
	}
}
