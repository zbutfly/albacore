package net.butfly.albacore.utils.async;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.utils.UtilsBase;
import net.butfly.albacore.utils.async.Task.ExceptionHandler;

import com.google.common.util.concurrent.MoreExecutors;

final class Tasks extends UtilsBase {
	static ExecutorService EXECUTOR = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors
			.newCachedThreadPool());
	static ExecutorService MORE_EX = Executors.newWorkStealingPool();

	static abstract class Handleable<T> extends ExceptionHandler<T> {
		protected ExceptionHandler<T> handler = new ExceptionHandler<T>();

		public T handle(Exception exception) throws Exception {
			if (null == handler) throw exception;
			else return handler.handle(exception);
		}
	}

	static <T> Task<T> wrapHandler(Task<T> task, ExceptionHandler<T> handler, Task.HandlerTarget... targets) {
		boolean undefined = targets == null || targets.length == 0;
		if (undefined || Arrays.binarySearch(targets, Task.HandlerTarget.CALLABLE) >= 0) task.call.handler = handler;
		if (!undefined && Arrays.binarySearch(targets, Task.HandlerTarget.CALLABLE) >= 0) task.back.handler = handler;
		return task;
	}

	static <T> T execute(final Task<T> task, ExecutorService executor) throws Exception {
		if (executor == null) executor = Tasks.EXECUTOR;
		if (task.options == null) task.options = new Options();

		final T result;
		Future<T> consumer;
		switch (task.options.mode) {
		case NONE:
			try {
				result = task.call.call();
			} catch (Exception ex) {
				return task.call.handle(ex);
			}
			try {
				return Tasks.callback(result, task.back);
			} catch (Exception ex) {
				return task.back.handle(ex);
			}
		case PRODUCER:
			try {
				result = Tasks.fetch(executor.submit(task.call), task.options.timeout);
			} catch (Exception ex) {
				return task.call.handle(ex);
			}
			try {
				return Tasks.callback(result, task.back);
			} catch (Exception ex) {
				return task.back.handle(ex);
			}
		case CONSUMER:
			try {
				result = task.call.call();
			} catch (Exception ex) {
				return task.call.handle(ex);
			}
			consumer = executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() throws Exception {
					try {
						return Tasks.callback(result, task.back);
					} catch (Exception ex) {
						return task.back.handle(ex);
					}
				}
			});
			if (task.options.unblock) return null;
			try {
				return consumer.get(task.options.timeout, TimeUnit.MILLISECONDS);
			} catch (Exception ex) {
				return task.call.handle(ex);
			}
		case LISTEN:
			final Future<T> producer = executor.submit(task.call);
			consumer = executor.submit(new java.util.concurrent.Callable<T>() {
				@Override
				public T call() throws Exception {
					T r;
					try {
						r = Tasks.fetch(producer, task.options.timeout);
					} catch (Exception ex) {
						return task.call.handle(ex);
					}
					try {
						return Tasks.callback(r, task.back);
					} catch (Exception ex) {
						return task.back.handle(ex);
					}
				}
			});
			if (!task.options.unblock) try {
				return Tasks.fetch(consumer, task.options.timeout);
			} catch (Exception ex) {
				return task.call.handle(ex);
			}
			return null;
		}
		throw new IllegalArgumentException();
	}

	private static <OUT> OUT callback(OUT result, Task.Callback<OUT> callback) throws Exception {
		if (null == callback) return result;
		callback.callback(result);
		return null;
	}

	private static <OUT> OUT fetch(Future<OUT> future, long timeout) throws Exception {
		try {
			return timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
		} catch (Exception e) {
			future.cancel(true);
			throw e;
		}
	}

}
