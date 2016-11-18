package net.butfly.albacore.io.pump;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import net.butfly.albacore.io.Queue;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Concurrents;

public class PumpBase<V> implements Pump<V> {
	private static final long serialVersionUID = 663917114528791086L;
	private static final int STATUS_OTHER = 0;
	private static final int STATUS_RUNNING = 1;
	private static final int STATUS_STOPPED = 2;

	protected final ListeningExecutorService ex;
	protected final List<Thread> threads = new ArrayList<>();
	protected List<ListenableFuture<?>> futures = new ArrayList<>();
	private long batchSize = 1000;
	private String name;
	private final AtomicInteger running;

	public PumpBase(Queue<?, V> source, Queue<V, ?> destination, int parallelism) {
		running = new AtomicInteger(STATUS_OTHER);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		this.name = source.name() + "-to-" + destination.name();
		logger.info("Pump [" + name + "] create: from [" + source.getClass().getSimpleName() + "] to [" + destination.getClass()
				.getSimpleName() + "] with parallelism: " + parallelism);
		Runnable r = Concurrents.until(//
				() -> running.get() != STATUS_RUNNING && (running.get() == STATUS_STOPPED || source.empty()), //
				() -> {
					try {
						destination.enqueue(stats(source.dequeue(batchSize)));
					} catch (Throwable th) {
						logger.error("Pump processing failure", th);
					}
				});
		for (int i = 0; i < parallelism; i++) {
			Thread t = new Thread(r, name + "[" + i + "]");
			threads.add(t);
		}
		ex = Concurrents.executor(parallelism, source.name(), destination.name());
	}

	@Override
	public Pump<V> batch(long batchSize) {
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
		if (running.getAndSet(STATUS_STOPPED) == STATUS_RUNNING) logger.info("Pump [" + name + "] terminating...");
		else logger.warn("Pump [" + name + "] is not running...");
		fs.cancel(true);
		logger.info("Pump [" + name + "] terminated.");
		ex.shutdown();
	}

	@Override
	public String toString() {
		return "Pump-" + name;
	}
}
