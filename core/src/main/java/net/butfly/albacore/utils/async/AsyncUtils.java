package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.exception.SystemExceptions;
import net.butfly.albacore.utils.UtilsBase;
import net.butfly.albacore.utils.async.Options.ForkMode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public final class AsyncUtils extends UtilsBase {
	private static ExecutorService EXECUTOR = Executors.newWorkStealingPool();
	private static ListeningExecutorService listenee = MoreExecutors.listeningDecorator(EXECUTOR);
	private static ListeningExecutorService listener = MoreExecutors.listeningDecorator(EXECUTOR);

	public static <OUT> OUT execute(final Task<OUT> task) throws Signal {
		return execute(EXECUTOR, task);
	}

	@Deprecated
	public static <OUT> OUT execute(final Task<OUT> task, final ExecutorService executor) throws Signal {
		return execute(executor, task);
	}

	private static <OUT> OUT execute(final ExecutorService executor, final Task<OUT> task) throws Signal {
		if (task.options() == null) task.options = new Options();
		if (task.options().mode() == ForkMode.NONE) {
			OUT result = task.task().call();
			if (null != task.callback()) {
				task.callback().callback(result);
				return null;
			} else return result;
		}
		if (task.options().mode() == ForkMode.PRODUCER)
			try {
				return fetch(executor.submit(wrap(task.task())), task.options().timeout());
			} catch (RejectedExecutionException e) {
				throw new SystemException(SystemExceptions.ASYNC_SATURATED,
						"async task executing rejected for pool saturated.", e);
			}
		if (task.options().mode() == ForkMode.CONSUMER)
			try {
				OUT result;
				result = task.task().call();
				assert (null == fetch(executor.submit(wrap(new Callable<OUT>() {
					@Override
					public OUT call() throws Signal {
						task.callback().callback(result);
						return null;
					}
				})), task.options().timeout()));
				return null;
			} catch (RejectedExecutionException e) {
				throw new SystemException(SystemExceptions.ASYNC_SATURATED,
						"async task executing rejected for pool saturated.", e);
			}

		// TODO: Use listenee/listener for executor, not default.
		ListenableFuture<OUT> future;
		try {
			future = listenee.submit(wrap(task.task()));
		} catch (RejectedExecutionException e) {
			throw new SystemException(SystemExceptions.ASYNC_SATURATED, "async task executing rejected for pool saturated.", e);
		}
		future.addListener(wrap(new Runnable() {
			@Override
			public void run() throws Throwable {
				task.callback().callback(fetch(future, task.options().timeout()));
			}
		}), listener);
		if (task.options().mode() == ForkMode.LISTEN) return null;
		if (task.options().mode() == ForkMode.BOTH_AND_WAIT) {
			while (!future.isCancelled() && !future.isDone())
				if (task.options().waiting() > 0) try {
					Thread.sleep(task.options().waiting());
				} catch (InterruptedException e) {
					throw new Signal.Completed(e);
				}
			// XXX
			return null;
			// if (future.isDone()) throw new Signal.Completed();
			// else throw new Signal.Completed(null);
		}
		throw new IllegalArgumentException();
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

	public static <R> java.util.concurrent.Callable<R> wrap(Callable<R> callback) {
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

	public static java.lang.Runnable wrap(Runnable runnable) {
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
}
