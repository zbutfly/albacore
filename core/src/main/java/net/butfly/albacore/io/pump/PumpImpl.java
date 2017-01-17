package net.butfly.albacore.io.pump;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Supplier;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.utils.async.Concurrents;

abstract class PumpImpl implements Pump {
	private static final long serialVersionUID = 5115726180980986678L;
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	private final int parallelism;
	// protected final AtomicInteger running;
	protected final List<OpenableThread> threads = new ArrayList<>();

	protected long batchSize = 1000;
	private final List<AutoCloseable> sources;
	private final List<AutoCloseable> destinations;

	protected PumpImpl(String name, int parallelism) {
		// running = new AtomicInteger(STATUS_OTHER);
		this.name = name;
		this.parallelism = parallelism;
		sources = new ArrayList<>();
		destinations = new ArrayList<>();
		logger.info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	protected final void sources(List<? extends AutoCloseable> sources) {
		this.sources.addAll(sources);
	}

	protected final void dests(List<? extends AutoCloseable> destinations) {
		this.destinations.addAll(destinations);
	}

	protected final void dests(AutoCloseable... destinations) {
		dests(Arrays.asList(destinations));
	}

	protected final void sources(AutoCloseable... sources) {
		sources(Arrays.asList(sources));
	}

	protected final void pumping(Supplier<Boolean> sourceEmpty, Runnable pumping) {
		Runnable r = Concurrents.until(//
				() -> !opened() && (closed() || sourceEmpty.get()), //
				() -> {
					try {
						pumping.run();
					} catch (Throwable th) {
						logger.error("Pump processing failure", th);
					}
				});
		for (int i = 0; i < parallelism; i++)
			threads.add(new OpenableThread(r, name + "-" + i));
	}

	@Override
	public final Pump batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public final void opening() {
		Pump.super.opening();
		for (OpenableThread t : threads)
			t.start();
		while (true)
			for (OpenableThread t : threads)
				if (t.isAlive() && !t.isInterrupted()) {
					Concurrents.waitSleep(500);
					continue;
				}
	}

	@Override
	public void close() {
		for (AutoCloseable dep : sources)
			try {
				dep.close();
			} catch (Exception e) {
				logger.error(dep.getClass().getName() + " close failed");
			}
		Pump.super.close();
		for (AutoCloseable dep : destinations)
			try {
				dep.close();
			} catch (Exception e) {
				logger.error(dep.getClass().getName() + " close failed");
			}
	}

	@Override
	public void closing() {
		for (OpenableThread t : threads)
			t.close();
	}

	@Override
	public void terminate() {
		for (OpenableThread t : threads)
			t.interrupt();
	}

	@Override
	public String toString() {
		return "Pump-" + name;
	}

	@Override
	public String name() {
		return name;
	}
}
