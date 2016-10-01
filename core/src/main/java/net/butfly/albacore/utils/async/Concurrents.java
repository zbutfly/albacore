package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Concurrents extends Utils {
	public static void waitFull(ExecutorService executor, Logger logger) {
		ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
		while (pool.getActiveCount() >= pool.getMaximumPoolSize()) {
			logger.trace("Executor [" + pool.getClass().getSimpleName() + "] is full, waiting...");
			waitSleep(1000);
		}
	}

	public static void waitShutdown(ExecutorService executor, long seconds, Logger logger) {
		executor.shutdown();
		boolean running = true;
		while (!running)
			try {
				logger.info("Waiting for executor terminate...");
				running = executor.awaitTermination(seconds, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.warn("Not all processing thread finished correctly, waiting interrupted.");
			}
	}

	public static void waitShutdown(ExecutorService executor, Logger logger) {
		waitShutdown(executor, 10, logger);
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
}
