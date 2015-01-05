package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.exception.SystemExceptions;
import net.butfly.albacore.utils.UtilsBase;

import com.google.common.util.concurrent.MoreExecutors;

public final class AsyncUtils extends UtilsBase {
	public static <OUT> OUT execute(final Task<OUT> task) throws Signal {
		return execute(EXECUTOR, task);
	}

	public static <OUT> OUT execute(final Task<OUT> task, final ExecutorService executor) throws Signal {
		return execute(executor == null ? EXECUTOR : executor, task);
	}

	private static <OUT> OUT execute(final ExecutorService executor, final Task<OUT> task) throws Signal {
		if (task.options == null) task.options = new Options();

		switch (task.options.mode) {
		case NONE:
			return callback(task.task.call(), task.callback);
		case PRODUCER:
			try {
				return callback(fetch(executor.submit(wrap(task.task)), task.options.timeout), task.callback);
			} catch (RejectedExecutionException e) {
				throw new SystemException(SystemExceptions.ASYNC_SATURATED,
						"async task executing rejected for pool saturated.", e);
			}
		case CONSUMER:
			final OUT result = task.task.call();
			{
				Future<OUT> consumer;
				try {
					consumer = executor.submit(wrap(new Callable<OUT>() {
						@Override
						public OUT call() throws Signal {
							return callback(result, task.callback);
						}
					}));
				} catch (RejectedExecutionException e) {
					throw new SystemException(SystemExceptions.ASYNC_SATURATED,
							"async task executing rejected for pool saturated.", e);
				}
				if (task.options.unblock) return null;
				try {
					return consumer.get(task.options.timeout, TimeUnit.MILLISECONDS);
				} catch (Exception e) {}
			}
		case LISTEN:
			final Future<OUT> producer;
			try {
				producer = executor.submit(wrap(task.task));
			} catch (RejectedExecutionException e) {
				throw new SystemException(SystemExceptions.ASYNC_SATURATED,
						"async task executing rejected for pool saturated.", e);
			}
			Future<OUT> consumer = executor.submit(wrap(new Callable<OUT>() {
				@Override
				public OUT call() throws Signal {
					return callback(fetch(producer, task.options.timeout), task.callback);
				}
			}));
			if (!task.options.unblock) return fetch(consumer, task.options.timeout);
			return null;
		}
		throw new IllegalArgumentException();
	}

	private static <OUT> OUT callback(OUT result, Callback<OUT> callback) throws Signal {
		if (null == callback) return result;
		callback.callback(result);
		return null;
	}

	private static <OUT> OUT fetch(Future<OUT> future, long timeout) throws Signal {
		try {
			return timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
		} catch (InterruptedException e) {
			future.cancel(true);
			throw new Signal.Completed(e);
		} catch (TimeoutException e) {
			future.cancel(true);
			throw new Signal.Timeout();
		} catch (ExecutionException e) {
			future.cancel(true);
			throw new Signal.Completed(e.getCause());
		}
	}

	public static <R> java.util.concurrent.Callable<R> wrap(final Callable<R> callback) {
		return new java.util.concurrent.Callable<R>() {
			@Override
			public R call() throws Exception {
				try {
					return callback.call();
				} catch (Throwable e) {
					if (e instanceof Exception) throw (Exception) e;
					else throw new Exception(e);
				}
			}
		};
	}

	public static java.lang.Runnable wrap(final Runnable runnable) {
		return new java.lang.Runnable() {
			@Override
			public void run() {
				try {
					runnable.run();
				} catch (Throwable e) {
					if (e instanceof RuntimeException) throw (RuntimeException) e;
					else throw new RuntimeException(e);
				}
			}
		};
	}

	class ResultFuture<V> implements Future<V> {
		private V result;

		public ResultFuture(V result) {
			this.result = result;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return true;
		}

		@Override
		public V get() throws InterruptedException, ExecutionException {
			return result;
		}

		@Override
		public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return result;
		}
	}

	private static ExecutorService EXECUTOR = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors
			.newCachedThreadPool());// Executors.newWorkStealingPool();

	public static Executor getDefaultExecutor() {
		return EXECUTOR;
	}
}
