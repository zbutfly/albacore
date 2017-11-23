package net.butfly.albacore.utils.parallel;

import static net.butfly.albacore.utils.parallel.Exeters.get;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

public final class Exeter extends AbstractExecutorService {
	static final Logger logger = Logger.getLogger(Exeter.class.getName());
	private final AbstractExecutorService impl;

	public <T> Future<T> submit(Supplier<T> task) {
		return impl.submit(() -> task.get());
	}

	public <T> List<Future<T>> submit(Collection<? extends Callable<T>> tasks) {
		try {
			return impl.invokeAll(tasks);
		} catch (InterruptedException e) {
			logger.severe("Tasks submit interrupted");
			return Arrays.asList();
		}
	}

	public <T> Future<?> submit(Runnable... tasks) {
		List<Future<?>> fs = new ArrayList<>();
		for (Runnable t : tasks)
			if (null != t) fs.add(impl.submit(t));
		return submit(() -> get(fs.toArray(new Future<?>[fs.size()])));
	}

	public <T> T join(Supplier<T> task) {
		return get(submit(task));
	}

	public <T> T join(Callable<T> task) {
		return get(impl.submit(task));
	}

	public <T> List<T> join(Collection<? extends Callable<T>> tasks) {
		return get(submit(tasks));
	}

	public void join(Runnable... tasks) {
		List<Future<?>> fs = new ArrayList<>();
		for (Runnable t : tasks)
			if (null != t) fs.add(impl.submit(t));
		get(fs.toArray(new Future<?>[fs.size()]));
	}

	public void join(Runnable task) {
		get(impl.submit(task));
	}

	// wrappers
	Exeter(AbstractExecutorService impl) {
		super();
		this.impl = impl;
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return impl.awaitTermination(timeout, unit);
	}

	@Override
	public boolean isShutdown() {
		return impl.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return impl.isTerminated();
	}

	@Override
	public void shutdown() {
		impl.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow() {
		return impl.shutdownNow();
	}

	@Override
	public void execute(Runnable command) {
		impl.execute(command);
	}

	@Override
	public String toString() {
		return String.valueOf(realExector(impl));
	}

	private static ExecutorService realExector(ExecutorService ex) {
		if (null == ex || ex instanceof ThreadPoolExecutor || ex instanceof ForkJoinPool) return ex;

		Object o;
		try {
			// DelegatedExecutorService
			Field f = ex.getClass().getDeclaredField("e");
			if (null == f) return ex;
			o = f.get(ex);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			return ex;
		}
		return null == o || !(o instanceof ExecutorService) ? ex : realExector((ExecutorService) o);
	}

	public int parallelism() {
		if (impl instanceof ForkJoinPool) return ((ForkJoinPool) impl).getParallelism();
		if (Exeters.DEF_EXECUTOR_PARALLELISM < 0) return -Exeters.DEF_EXECUTOR_PARALLELISM;
		return Integer.MAX_VALUE;
	}
}
