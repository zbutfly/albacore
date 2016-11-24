package net.butfly.albacore.io.pump;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import net.butfly.albacore.utils.async.Concurrents;

public abstract class PumpImpl<V> implements Pump<V> {
	private static final long serialVersionUID = 5115726180980986678L;
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	private final int parallelism;
	protected final AtomicInteger running;
	protected final List<Thread> threads = new ArrayList<>();
	protected final ListeningExecutorService ex;
	protected final List<ListenableFuture<?>> futures = new ArrayList<>();

	protected long batchSize = 1000;

	protected PumpImpl(String name, int parallelism) {
		running = new AtomicInteger(STATUS_OTHER);
		this.name = name;
		this.parallelism = parallelism;
		ex = Concurrents.executor(parallelism, name);
		logger.info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	protected void pumping(Supplier<Boolean> sourceEmpty, Runnable pumping) {
		Runnable r = Concurrents.until(//
				() -> running.get() != STATUS_RUNNING && (running.get() == STATUS_STOPPED || sourceEmpty.get()), //
				() -> {
					try {
						pumping.run();
					} catch (Throwable th) {
						logger.error("Pump processing failure", th);
					}
				});
		for (int i = 0; i < parallelism; i++) {
			Thread t = new Thread(r, name + "[" + i + "]");
			threads.add(t);
		}
	}

	@Override
	public final Pump<V> batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	Pump<V> start() {
		running.set(STATUS_RUNNING);
		logger.info("Pump [" + name + "] starting...");
		for (Thread t : threads)
			futures.add(ex.submit(t));
		logger.info("Pump [" + name + "] started.");
		return this;
	}

	void waitForFinish(long timeout, TimeUnit unit) {
		running.set(STATUS_OTHER);
		logger.info("Pump [" + name + "] stopping in [" + timeout + " " + unit.toString() + "]...");
		try {
			Futures.successfulAsList(futures).get(timeout, unit);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("Pump waiting failure", e);
		}
		logger.info("Pump [" + name + "] stopped.");
		running.set(STATUS_STOPPED);
		ex.shutdown();
	}

	void waitForFinish() {
		running.set(STATUS_OTHER);
		logger.info("Pump [" + name + "] stopping...");
		try {
			Futures.successfulAsList(futures).get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Pump waiting failure", e);
		}
		logger.info("Pump [" + name + "] stopped.");
		running.set(STATUS_STOPPED);
		ex.shutdown();
	}

	void terminate() {
		ListenableFuture<List<Object>> fs = Futures.allAsList(futures);
		int status = running.getAndSet(STATUS_STOPPED);
		if (status == STATUS_RUNNING) logger.info("Pump [" + name + "] terminating...");
		else logger.warn("Pump [" + name + "] is not running...");
		fs.cancel(true);
		if (status == STATUS_RUNNING) logger.info("Pump [" + name + "] terminated.");
		ex.shutdownNow();
	}

	@Override
	public String toString() {
		return "Pump-" + name;
	}
}
