package net.butfly.albacore.utils.async;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.utils.ExceptionUtils;
import net.butfly.albacore.utils.UtilsBase;

import com.google.common.util.concurrent.MoreExecutors;

public final class AsyncUtils extends UtilsBase {
	static ExecutorService EXECUTOR = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors
			.newCachedThreadPool());// Executors.newWorkStealingPool();

	static <OUT> OUT callback(OUT result, Task.Callback<OUT> callback) throws Exception {
		if (null == callback) return result;
		callback.callback(result);
		return null;
	}

	static <OUT> OUT fetch(Future<OUT> future, long timeout) throws Exception {
		try {
			return timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
		} catch (Exception e) {
			future.cancel(true);
			throw ExceptionUtils.wrap(e);
		}
	}

	public static <R> Callable<R> wrap(final Task.Callable<R> callback) {
		return new Callable<R>() {
			@Override
			public R call() throws Exception {
				try {
					return callback.call();
				} catch (Exception e) {
					if (e instanceof Exception) throw (Exception) e;
					else throw new Exception(e);
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

	public static Executor getDefaultExecutor() {
		return EXECUTOR;
	}

}
