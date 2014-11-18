package net.butfly.albacore.utils.async;

import java.util.concurrent.Callable;
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

	public static <OUT> OUT execute(final Task<OUT> task) {
		return execute(EXECUTOR, task);
	}

	public static <OUT> OUT execute(final ExecutorService executor, final Task<OUT> task) {
		if (task.options().mode() == ForkMode.NONE) {
			try {
				OUT result = task.task().call();
				if (null != task.callback()) task.callback().callback(result);
				return result;
			} catch (SystemException e) {
				throw e;
			} catch (Exception e) {
				throw new SystemException("", e);
			}
		}
		if (task.options().mode() == ForkMode.PRODUCER)
			try {
				return fetch(executor.submit(task.task()), task.options().timeout());
			} catch (RejectedExecutionException e) {
				throw new SystemException(SystemExceptions.ASYNC_SATURATED,
						"async task executing rejected for pool saturated.", e);
			}
		if (task.options().mode() == ForkMode.CONSUMER)
			try {
				OUT result;
				try {
					result = task.task().call();
				} catch (Exception e) {
					throw new SystemException("", e);
				}
				fetch(executor.submit(new Callable<OUT>() {
					@Override
					public OUT call() throws Exception {
						task.callback().callback(result);
						return result;
					}
				}), task.options().timeout());
				return result;
			} catch (RejectedExecutionException e) {
				throw new SystemException(SystemExceptions.ASYNC_SATURATED,
						"async task executing rejected for pool saturated.", e);
			}
		ListenableFuture<OUT> future;
		try {
			future = listenee.submit(task.task());
		} catch (RejectedExecutionException e) {
			throw new SystemException(SystemExceptions.ASYNC_SATURATED, "async task executing rejected for pool saturated.", e);
		}
		future.addListener(new Runnable() {
			@Override
			public void run() {
				task.callback().callback(fetch(future, task.options().timeout()));
			}
		}, listener);
		if (task.options().mode() == ForkMode.LISTEN) throw new Signal.Completed();
		if (task.options().mode() == ForkMode.BOTH_AND_WAIT) {
			while (!future.isCancelled() && !future.isDone())
				if (task.options().waiting() > 0) try {
					Thread.sleep(task.options().waiting());
				} catch (InterruptedException e) {
					throw new Signal.Completed(e);
				}
			// XXX
			if (future.isDone()) throw new Signal.Completed();
			else throw new Signal.Completed(null);
		}
		throw new IllegalArgumentException();
	}

	private static <OUT> OUT fetch(Future<OUT> future, long timeout) {
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
}
