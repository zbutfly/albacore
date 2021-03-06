package net.butfly.albacore.utils.logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LogExec {
	private static final String PROP_LOGGER_ASYNC = "albacore.logger.async.enable";// true
	private static final String PROP_LOGGER_PARALLELISM = "albacore.logger.parallelism";// true
	private static final String PROP_LOGGER_QUEUE_SIZE = "albacore.logger.queue.size";// true
	private static final ExecutorService logex;
	static {
		if (Boolean.parseBoolean(System.getProperty(PROP_LOGGER_ASYNC, "true"))) {
			int parallelism = Integer.parseInt(System.getProperty(PROP_LOGGER_PARALLELISM, "8"));
			int queueSize = Integer.parseInt(System.getProperty(PROP_LOGGER_QUEUE_SIZE, "1024"));
			AtomicInteger tn = new AtomicInteger();
			ThreadGroup g = new ThreadGroup("AlbacoreLoggerThread");
			logex = new ThreadPoolExecutor(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS, //
					new LinkedBlockingQueue<Runnable>(queueSize), r -> {
						Thread t = new Thread(g, r, "AlbacoreLoggerThread#" + tn.getAndIncrement());
						t.setDaemon(true);
						return t;
					}, (r, ex) -> {
						// process rejected...ignore
					});
		} else {
			logex = null;
		}
	}

	public static boolean tryExec(Runnable r) {
		try {
			logex.execute(r);
			return true;
		} catch (RejectedExecutionException e) {
			return false;
		}
	}
}
