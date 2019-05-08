package net.butfly.albacore.paral;

import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;
import static java.util.concurrent.ForkJoinPool.getCommonPoolParallelism;
import static net.butfly.albacore.Albacore.Props.PROP_PARALLEL_FACTOR;
import static net.butfly.albacore.paral.Exeter.logger;
import static net.butfly.albacore.paral.Exeter.tracePool;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.Texts;

class ExeterHandler implements RejectedExecutionHandler, UncaughtExceptionHandler, ThreadFactory {
//	private static final Map<String, ThreadGroup> g = Maps.of();
	final String name;
	private final boolean throwException;

	ExeterHandler(String name, boolean throwException) {
		super();
		this.name = name;
		this.throwException = throwException;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(/* g.computeIfAbsent(name, n -> name + "ThreadGroup")), */r);
		t.setName(name + "#" + t.getId() + "@" + Texts.iso8601(new Date()) + "");
		t.setUncaughtExceptionHandler(this);
		return t;
	}

	public ForkJoinWorkerThread newForkjoin(ForkJoinPool pool) {
		ForkJoinWorkerThread t = defaultForkJoinWorkerThreadFactory.newThread(pool);
		t.setName(name + "#" + t.getId() + "@" + Texts.iso8601(new Date()) + "");
		return t;
	}

	@Override
	public synchronized void rejectedExecution(Runnable r, ThreadPoolExecutor ex) {
		if (Systems.isDebug()) logger.error("Task rejected, " + tracePool(ex));
		else throw new RejectedExecutionException("Task rejected, " + tracePool(ex));
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		logger.error("Migrater pool task failure @" + t.getName(), e);
		if (throwException) throw new RuntimeException(e);
	}

	// Utils
	private static final String DEF_EXECUTOR_NAME = "AlExeter";
	static final int DEF_PARALLELISM = detectParallelism();
	static final WrapperExeter DEF_EX = Exeter.newExecutor(DEF_EXECUTOR_NAME, //
			Integer.parseInt(Configs.gets(Albacore.Props.PROP_PARALLELISM, Integer.toString(DEF_PARALLELISM))));

	ExecutorService debugExec(int parallelism) {
		logger.warn("Thread executor for debugging, slow and maybe locked...\n\tuse -Dalbacore.debug=false to override auto detection.");
		switch (parallelism) {
		case 1: // Fixed but less
			logger.info("Executor [minor Fixed: " + getCommonPoolParallelism() + " for debug] create, prefix: " + name);
			return new ThreadPoolExecutor(getCommonPoolParallelism(), getCommonPoolParallelism(), 0L, TimeUnit.MILLISECONDS,
					new LinkedBlockingQueue<Runnable>(), this, this);
		case 0: // Calced ForkJoin
			logger.info("Executor [ForkJoin: " + DEF_PARALLELISM + " for debug] create, prefix: " + name);
			return new ForkJoinPool(DEF_PARALLELISM, this::newForkjoin, this, false);
		default: // default
			logger.info("Executor [ForkJoin: " + Math.abs(parallelism) + " for debug] create, prefix: " + name);
			return new ForkJoinPool(Math.abs(parallelism), this::newForkjoin, this, false);
		}
	}

	ExecutorService runExec(int parallelism) {
		switch (parallelism) {
		case 1: // Calced ForkJoin, default
			logger.info("Executor [ForkJoin: " + DEF_PARALLELISM + "] create, prefix: " + name);
			return new ForkJoinPool(DEF_PARALLELISM, this::newForkjoin, this, true);
		case 0: // Cached
			logger.info("Executor [Unlimited Cached] create, prefix: " + name);
			return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 30L, TimeUnit.SECONDS, new SynchronousQueue<>(), this, this);
		default:
			if (parallelism > 0) {// Given value Fixed
				logger.info("Executor [Fixed: " + parallelism + "] create, prefix: " + name);
				return new ThreadPoolExecutor(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), this,
						this);
			} else { // Given value ForkJoinPool
				logger.info("Executor [ForkJoin: " + (-parallelism) + "] create, prefix: " + name);
				return new ForkJoinPool(-parallelism, this::newForkjoin, this, true);
			}
		}
	}

	private static int detectParallelism() {
		int fp = getCommonPoolParallelism();
		logger.debug("ForkJoinPool.getCommonPoolParallelism(): " + fp);
		double f = Double.parseDouble(Configs.gets(PROP_PARALLEL_FACTOR, "1"));
		if (f <= 0) return (int) f;
		int p = 16 + (int) Math.round((fp - 16) * (f - 1));
		if (p < 2) p = 2;
		if (p > 0x7fff) p = 0x7fff;
		logger.info("AlbacoreIO parallelism adjusted to [" + p + "] by [-D" + PROP_PARALLEL_FACTOR + "=" + f + "].");
		return p;
	}
}
