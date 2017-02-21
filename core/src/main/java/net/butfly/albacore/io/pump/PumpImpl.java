package net.butfly.albacore.io.pump;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Supplier;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.lambda.Runnable;

abstract class PumpImpl<V, P extends PumpImpl<V, P>> extends Namedly implements Pump<V> {
	protected static final int STATUS_OTHER = 0;
	protected static final int STATUS_RUNNING = 1;
	protected static final int STATUS_STOPPED = 2;

	protected final String name;
	// private final int parallelism;
	private final List<Runnable> tasks = new ArrayList<>();

	protected long batchSize = 1000;
	private final List<AutoCloseable> dependencies;

	protected PumpImpl(String name, int parallelism) {
		super(name);
		this.name = name;
		// this.parallelism = parallelism;
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
		Runnable rr = r.until(() -> {
			return !opened() || sourceEmpty.get();
		});
		for (int i = 0; i < IO.io.parallelism(); i++)
			tasks.add(rr);
	}

	@Override
	public final PumpImpl<V, P> batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	@Override
	public void open(java.lang.Runnable run) {
		Pump.super.open(run);
		try {
			IO.io.listenRun(tasks).get();
		} catch (InterruptedException e) {} catch (Exception e) {
			throw wrap(unwrap(e));
		}
		logger().info(name() + " finished.");
		for (AutoCloseable dep : dependencies)
			try {
				dep.close();
			} catch (Exception e) {
				logger().error(dep.getClass().getName() + " close failed");
			}
	}

	@Override
	public void close() {
		Pump.super.close();
	}
}
