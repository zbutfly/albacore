package net.butfly.albacore.paral;

import static net.butfly.albacore.paral.Exeter.get;
import static net.butfly.albacore.paral.Exeter.Internal.DEF_EXECUTOR_PARALLELISM;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

class WrapperExeter implements Exeter {
	private final ExecutorService impl;

	@Override
	public String toString() {
		return String.valueOf(realExector(impl));
	}

	@Override
	public int parallelism() {
		if (impl instanceof ForkJoinPool) return ((ForkJoinPool) impl).getParallelism();
		if (DEF_EXECUTOR_PARALLELISM < 0) return -DEF_EXECUTOR_PARALLELISM;
		return Integer.MAX_VALUE;
	}

	@Override
	public Future<?> collect(final Iterable<Future<?>> futures) {
		return submit(() -> {
			for (final Future<?> f : futures)
				get(f);
		});
	}

	@Override
	public <T> Future<T> submit(final Supplier<T> task) {
		return submit((Callable<T>) () -> task.get());
	}

	@Override
	public <T> List<Future<T>> submit(final Collection<? extends Callable<T>> tasks) {
		try {
			return invokeAll(tasks);
		} catch (final InterruptedException e) {
			logger.error("Tasks submit interrupted");
			return Arrays.asList();
		}
	}

	@Override
	public <T> Future<?> submit(final Runnable... tasks) {
		final List<Future<?>> fs = new ArrayList<>();
		for (final Runnable t : tasks)
			if (null != t) fs.add(submit(t));
		return submit(() -> get(fs.toArray(new Future<?>[fs.size()])));
	}

	@Override
	public <T> T join(final Supplier<T> task) {
		return get(submit(task));
	}

	@Override
	public <T> T join(final Callable<T> task) {
		return get(submit(task));
	}

	@Override
	public <T> List<T> join(final Collection<? extends Callable<T>> tasks) {
		return get(submit(tasks));
	}

	@Override
	public void join(final Runnable... tasks) {
		final List<Future<?>> fs = new ArrayList<>();
		for (final Runnable t : tasks)
			if (null != t) fs.add(submit(t));
		get(fs.toArray(new Future<?>[fs.size()]));
	}

	@Override
	public void join(final Runnable task) {
		get(submit(task));
	}

	// wrappers
	WrapperExeter(final ExecutorService impl) {
		super();
		this.impl = impl;
	}

	private static ExecutorService realExector(final ExecutorService ex) {
		if (null == ex || ex instanceof ThreadPoolExecutor || ex instanceof ForkJoinPool) return ex;
		Object o;
		try {
			// DelegatedExecutorService
			final Field f = ex.getClass().getDeclaredField("e");
			if (null == f) return ex;
			o = f.get(ex);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			return ex;
		}
		return null == o || !(o instanceof ExecutorService) ? ex : realExector((ExecutorService) o);
	}

	// ===========================
	@Override
	public void execute(final Runnable command) {
		impl.execute(command);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return impl.invokeAll(tasks);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
		return impl.invokeAll(tasks, timeout, unit);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		return impl.invokeAny(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		return impl.invokeAny(tasks, timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		return impl.submit(task);
	}

	@Override
	public Future<?> submit(Runnable task) {
		return impl.submit(task);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		return impl.submit(task, result);
	}

	// ===========================
	@Override
	public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
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
}
