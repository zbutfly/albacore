package net.butfly.albacore.utils.async;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.exception.AggregaedException;
import net.butfly.albacore.lambda.Callable;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Tasks extends Utils {
	private static final Logger logger = Logger.getLogger(Tasks.class);

	@SuppressWarnings("unchecked")
	public static <T> T[] executeSequential(ExecutorService executor, Class<T> targetClass, final List<Callable<T>> tasks) {
		List<T> results = new ArrayList<T>();
		List<Throwable> errors = new ArrayList<Throwable>();
		for (Callable<T> t : tasks)
			try {
				results.add(t.call());
			} catch (Exception e) {
				errors.add(e.getCause());
				logger.error("Sliced task failed at slices.", e.getCause());
			}
		if (!errors.isEmpty()) logger.error("Error in concurrence", new AggregaedException("", "Error in concurrence", errors.toArray(
				new Throwable[errors.size()])));
		T[] r = results.toArray((T[]) Array.newInstance(targetClass, results.size()));
		return r;
	}

	@SuppressWarnings("unchecked")
	public static <T> T[] executeConcurrently(final ExecutorService executor, Class<T> targetClass,
			final List<? extends Callable<T>> tasks) {
		List<T> results = new ArrayList<T>();
		List<Throwable> errors = new ArrayList<Throwable>();
		CompletionService<T> cs = Instances.fetch(() -> new ExecutorCompletionService<T>(executor), CompletionService.class, executor);
		for (Callable<T> t : tasks) {
			cs.submit(t);
		}
		for (int i = 0; i < tasks.size(); i++) {
			try {
				results.add(cs.take().get());
			} catch (InterruptedException e) {
				logger.error("Sliced task interrupted at " + i + "th slice.", e);
			} catch (ExecutionException e) {
				errors.add(e.getCause());
				logger.error("Sliced task failed at " + i + "th slice.", e.getCause());
			}
		}
		if (!errors.isEmpty()) logger.error("Error in concurrence", new AggregaedException("", "Error in concurrence", errors.toArray(
				new Throwable[errors.size()])));
		T[] r = results.toArray((T[]) Array.newInstance(targetClass, results.size()));
		return r;
	}

	static <T> T execute(final Task<T> task, ExecutorService executor) throws Exception {
		if (executor == null) executor = Concurrents.executor();
		if (task.options == null) task.options = new Options();
		int repeated = 0, retried = 0;
		T result = null;
		while ((task.options.repeat < 0 || repeated < task.options.repeat) && retried <= task.options.retry) {
			try {
				result = single(task, executor);
				repeated++;
			} catch (Exception ex) {
				result = handle(task, ex);
				retried++;
			}
			Concurrents.waitSleep(task.options.interval);
		}
		return result;
	}

	private static <T> T single(final Task<T> task, ExecutorService executor) throws Exception {
		switch (task.options.mode) {
		case NONE:
			return callback(task.call.call(), task.back);
		case WHOLE:
			return fetch(task, executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() throws Exception {
					try {
						return callback(task.call.call(), task.back);
					} catch (Exception ex) {
						return handle(task, ex);
					}
				}
			}));
		case LATTER:
			final T result = task.call.call();
			return fetch(task, executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() {
					return callback(result, task.back);
				}
			}));
		case EACH:
			final Future<T> producer = executor.submit(task.call);
			final Future<T> consumer = executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() throws Exception {
					T r;
					try {
						r = fetch(producer, task.options.timeout);
					} catch (Exception ex) {
						return handle(task, ex);
					}
					return callback(r, task.back);
				}
			});
			return fetch(task, consumer);
		default:
			throw new IllegalArgumentException();
		}
	}

	private static <OUT> OUT handle(Task<OUT> task, Exception ex) throws Exception {
		if (null == task.handler) throw ex;
		return task.handler.handle(ex);
	}

	private static <OUT> OUT callback(OUT result, Consumer<OUT> callback) {
		if (null == callback) return result;
		callback.accept(result);
		return null;
	}

	private static <OUT> OUT fetch(final Task<OUT> task, Future<OUT> future) throws Exception {
		if (task.options.unblock) return null;
		else try {
			return fetch(future, task.options.timeout);
		} catch (Exception ex) {
			return handle(task, ex);
		}
	}

	private static <OUT> OUT fetch(Future<OUT> future, long timeout) throws InterruptedException, ExecutionException, TimeoutException {
		try {
			return timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
		} catch (InterruptedException e) {
			future.cancel(true);
			throw e;
		} catch (ExecutionException e) {
			future.cancel(true);
			throw e;
		} catch (TimeoutException e) {
			future.cancel(true);
			throw e;
		}
	}

}
