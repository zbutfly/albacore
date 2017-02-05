package net.butfly.albacore.io.pump;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Supplier;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.async.Concurrents;

abstract class PumpImpl<V> extends Namedly implements Pump<V> {
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	private final int parallelism;
	protected final Map<String, OpenableThread> threads = new ConcurrentHashMap<>();
	protected final ThreadGroup threadGroup;

	protected long batchSize = 1000;
	private final List<AutoCloseable> dependencies;

	protected PumpImpl(String name, int parallelism) {
		super(name);
		this.name = name;
		this.parallelism = parallelism;
		threadGroup = new ThreadGroup(name);
		dependencies = new ArrayList<>();
		logger().info("Pump [" + name + "] created with parallelism: " + parallelism);
	}

	protected final void depend(List<? extends AutoCloseable> dependencies) {
		this.dependencies.addAll(dependencies);
	}

	protected final void depend(AutoCloseable... dependencies) {
		depend(Arrays.asList(dependencies));
	}

	protected final void pumping(Supplier<Boolean> sourceEmpty, Runnable pumping) {
		Runnable r = () -> {
			try {
				pumping.run();
			} catch (Throwable th) {
				logger().error("Pump processing failure", th);
			}
		};
		for (int i = 0; i < parallelism; i++) {
			String threadName = name + "#" + (i + 1);
			threads.put(threadName, new OpenableThread(threadGroup, r.until(() -> {
				return !opened() || sourceEmpty.get();
			}).then(() -> {
				threads.remove(threadName);
			}), threadName));
		}
	}

	@Override
	public final Pump<V> batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public void open(java.lang.Runnable run) {
		Pump.super.open(run);
		for (OpenableThread t : threads.values())
			t.open();
		while (opened() && !threads.isEmpty())
			Concurrents.waitSleep(500);
		logger().info(name() + " finished.");
	}

	@Override
	public void close() {
		Pump.super.close();
		for (OpenableThread t : threads.values())
			t.close();
		for (AutoCloseable dep : dependencies)
			try {
				dep.close();
			} catch (Exception e) {
				logger().error(dep.getClass().getName() + " close failed");
			}
	}
}
