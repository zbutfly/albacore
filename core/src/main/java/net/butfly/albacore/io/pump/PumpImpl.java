package net.butfly.albacore.io.pump;

import java.util.ArrayList;
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
	private final List<AutoCloseable> dependencies;

	protected PumpImpl(String name, int parallelism) {
		// running = new AtomicInteger(STATUS_OTHER);
		this.name = name;
		this.parallelism = parallelism;
		dependencies = new ArrayList<>();
		logger.info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	protected final void depdenencies(List<? extends AutoCloseable> depdenencies) {
		this.dependencies.addAll(depdenencies);
	}

	protected final void depdenencies(AutoCloseable... depdenencies) {
		for (AutoCloseable dep : dependencies)
			this.dependencies.add(dep);
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
		for (int i = 0; i < parallelism; i++)
			threads.add(new OpenableThread(r, name + "-" + i));
	}

	@Override
	public final Pump batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public void opening() {
		Pump.super.opening();
		for (OpenableThread t : threads)
			t.start();
	}

	@Override
	public void closing() {
		Pump.super.closing();
		for (OpenableThread t : threads)
			t.close();
		for (AutoCloseable dep : dependencies)
			try {
				dep.close();
			} catch (Exception e) {}
	}

	@Override
	public void terminate() {
		for (OpenableThread t : threads)
			t.interrupt();
		close();
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
