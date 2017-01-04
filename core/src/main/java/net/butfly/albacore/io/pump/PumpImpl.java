package net.butfly.albacore.io.pump;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import net.butfly.albacore.utils.async.Concurrents;

abstract class PumpImpl implements Pump {
	private static final long serialVersionUID = 5115726180980986678L;
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	private final int parallelism;
	// protected final AtomicInteger running;
	protected final List<Thread> threads = new ArrayList<>();
	protected final ListeningExecutorService ex;

	protected long batchSize = 1000;
	private ListenableFuture<List<Object>> results;

	protected PumpImpl(String name, int parallelism) {
		// running = new AtomicInteger(STATUS_OTHER);
		this.name = name;
		this.parallelism = parallelism;
		ex = Concurrents.executor(parallelism, name);
		logger.info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	protected void pumping(Supplier<Boolean> sourceEmpty, Runnable pumping) {
		Runnable r = Concurrents.until(//
				() -> !opened() && (closed() || sourceEmpty.get()), //
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
	public final Pump batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public void opening() {
		open();
		logger.info("Pump [" + name + "] starting...");
		List<ListenableFuture<?>> futures = new ArrayList<>();
		for (Thread t : threads)
			futures.add(ex.submit(t));
		results = Futures.allAsList(futures);
		logger.info("Pump [" + name + "] started.");
	}

	@Override
	public void closing() {
		Pump.super.closing();
		try {
			results.get();
		} catch (InterruptedException e) {
			logger.warn("Pump interrupted.");
		} catch (ExecutionException e) {
			logger.error("Pump waiting failure", e.getCause());
		}
		ex.shutdown();
	}

	void terminate() {
		Pump.super.close(() -> {
			results.cancel(true);
			ex.shutdownNow();
		});
	}

	@Override
	public String toString() {
		return "Pump-" + name;
	}
}
