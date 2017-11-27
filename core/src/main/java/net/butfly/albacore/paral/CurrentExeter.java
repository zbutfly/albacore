package net.butfly.albacore.paral;

import static net.butfly.albacore.paral.Parals.list;
import static net.butfly.albacore.paral.Futures.done;
import static net.butfly.albacore.paral.Futures.fail;
import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.paral.Exeter.get;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;

public final class CurrentExeter implements Exeter {
	@Override
	public boolean awaitTermination(long arg0, TimeUnit arg1) throws InterruptedException {
		return true;
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		List<Future<T>> l = list();
		for (Callable<T> t : tasks)
			l.add(submit(t));
		return l;
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long arg1, TimeUnit arg2) throws InterruptedException {
		return invokeAll(tasks);
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		if (tasks.isEmpty()) return null;
		else return submit(tasks.iterator().next()).get();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long arg1, TimeUnit arg2) throws InterruptedException,
			ExecutionException, TimeoutException {
		return invokeAny(tasks);
	}

	@Override
	public boolean isShutdown() {
		return false;
	}

	@Override
	public boolean isTerminated() {
		return false;
	}

	@Override
	public void shutdown() {}

	@Override
	public List<Runnable> shutdownNow() {
		return list();
	}

	@Override
	public <T> Future<T> submit(Callable<T> t) {
		try {
			return done(t.call());
		} catch (Exception e) {
			return fail(e);
		}
	}

	@Override
	public Future<?> submit(Runnable t) {
		return submit(t, null);
	}

	@Override
	public <T> Future<T> submit(Runnable t, T result) {
		try {
			t.run();
			return done(result);
		} catch (Exception e) {
			return fail(e);
		}
	}

	@Override
	public void execute(Runnable t) {
		t.run();
	}

	@Override
	public int parallelism() {
		return 1;
	}

	@Override
	public Future<?> collect(Iterable<Future<?>> futures) {
		for (Future<?> f : futures)
			try {
				get(f);
			} catch (Exception ex) {
				return fail(ex);
			}
		return done(null);
	}

	@Override
	public <T> Future<T> submit(Supplier<T> task) {
		try {
			return done(task.get());
		} catch (Exception ex) {
			return fail(ex);
		}
	}

	@Override
	public <T> List<Future<T>> submit(Collection<? extends Callable<T>> tasks) {
		try {
			return invokeAll(tasks);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T> Future<?> submit(Runnable... tasks) {
		for (Runnable t : tasks)
			t.run();
		return done(null);
	}

	@Override
	public <T> T join(Supplier<T> task) {
		return task.get();
	}

	@Override
	public <T> T join(Callable<T> task) {
		try {
			return task.call();
		} catch (Exception e) {
			logger.log(Level.SEVERE, unwrap(e), () -> "Subtask error");
			return null;
		}
	}

	@Override
	public <T> List<T> join(Collection<? extends Callable<T>> tasks) {
		List<T> l = list();
		for (Callable<T> t : tasks)
			try {
				l.add(t.call());
			} catch (Exception e) {
				logger.log(Level.SEVERE, unwrap(e), () -> "Subtask error");
			}
		return l;
	}

	@Override
	public void join(Runnable... tasks) {
		for (Runnable t : tasks)
			t.run();
	}

	@Override
	public void join(Runnable task) {
		task.run();
	}
}
