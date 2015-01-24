package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.utils.UtilsBase;

import com.google.common.util.concurrent.MoreExecutors;

final class Tasks extends UtilsBase {
	// static ExecutorService MORE_EX = Executors.newWorkStealingPool();
	static ExecutorService EXECUTOR = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors
			.newCachedThreadPool());

	static <T> T execute(final Task<T> task, ExecutorService executor) throws Exception {
		if (executor == null) executor = EXECUTOR;
		if (task.options == null) task.options = new Options();
		final T result;
		Future<T> consumer;
		switch (task.options.mode) {
		case NONE:
			try {
				result = task.call.call();
			} catch (Exception ex) {
				return handle(task, ex);
			}
			return callback(result, task.back);
		case WHOLE:
			consumer = executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() throws Exception {
					try {
						return callback(task.call.call(), task.back);
					} catch (Exception ex) {
						return handle(task, ex);
					}
				}
			});
			return fetch(task, consumer);
		case LATTER:
			try {
				result = task.call.call();
			} catch (Exception ex) {
				return handle(task, ex);
			}
			consumer = executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() {
					return callback(result, task.back);
				}
			});
			return fetch(task, consumer);
		case EACH:
			final Future<T> producer = executor.submit(task.call);
			consumer = executor.submit(new java.util.concurrent.Callable<T>() {
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

	private static <OUT> OUT callback(OUT result, Task.Callback<OUT> callback) {
		if (null == callback) return result;
		callback.callback(result);
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

	private static <OUT> OUT fetch(Future<OUT> future, long timeout) throws InterruptedException, ExecutionException,
			TimeoutException {
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
