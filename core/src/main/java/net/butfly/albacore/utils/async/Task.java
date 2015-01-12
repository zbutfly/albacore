package net.butfly.albacore.utils.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.exception.AsyncException;

public class Task<T> {
	protected Callable<T> callable;
	protected Callback<T> callback;
	protected Callback<Exception> exception = null;
	protected Options options;

	public interface Callback<R> {
		void callback(final R result) throws Exception;
	}

	public interface Callable<R> {
		R call() throws Exception;
	}

	protected Task() {}

	public Task(Callable<T> task) {
		this(task, null, null);
	}

	public Task(Callable<T> task, Callback<T> callback) {
		this(task, callback, null);
	}

	public Task(Callable<T> task, Options options) {
		this(task, null, options);
	}

	public Task(Callable<T> task, Callback<T> callback, Options options) {
		this.callable = task;
		this.callback = callback;
		this.options = options;
	}

	public Callable<T> task() {
		return callable;
	}

	public Callback<T> callback() {
		return callback;
	}

	public Options options() {
		return options;
	}

	public Callback<Exception> exception() {
		return exception;
	}

	public Task<T> exception(Callback<Exception> exception) {
		this.exception = exception;
		return this;
	}

	public T execute() throws Exception {
		return execute(AsyncUtils.EXECUTOR);
	}

	public T execute(ExecutorService executor) throws Exception {
		if (this.exception != null) return this.wrapExceptionHandler().execute();
		if (executor == null) executor = AsyncUtils.EXECUTOR;
		if (options == null) options = new Options();

		switch (options.mode) {
		case NONE:
			return AsyncUtils.callback(callable.call(), callback);
		case PRODUCER:
			try {
				return AsyncUtils.callback(AsyncUtils.fetch(executor.submit(AsyncUtils.wrap(callable)), options.timeout),
						callback);
			} catch (RejectedExecutionException e) {
				throw new AsyncException("async callable executing rejected for pool saturated.", e);
			}
		case CONSUMER:
			final T result = callable.call();
			{
				Future<T> consumer;
				try {
					consumer = executor.submit(AsyncUtils.wrap(new Task.Callable<T>() {
						@Override
						public T call() throws Exception {
							return AsyncUtils.callback(result, callback);
						}
					}));
				} catch (RejectedExecutionException e) {
					throw new AsyncException("async callable executing rejected for pool saturated.", e);
				}
				if (options.unblock) return null;
				try {
					return consumer.get(options.timeout, TimeUnit.MILLISECONDS);
				} catch (Exception e) {}
			}
		case LISTEN:
			final Future<T> producer;
			try {
				producer = executor.submit(AsyncUtils.wrap(callable));
			} catch (RejectedExecutionException e) {
				throw new AsyncException("async callable executing rejected for pool saturated.", e);
			}
			Future<T> consumer = executor.submit(AsyncUtils.wrap(new Task.Callable<T>() {
				@Override
				public T call() throws Exception {
					return AsyncUtils.callback(AsyncUtils.fetch(producer, options.timeout), callback);
				}
			}));
			if (!options.unblock) return AsyncUtils.fetch(consumer, options.timeout);
			return null;
		}
		throw new IllegalArgumentException();
	}

	private Task<T> wrapExceptionHandler() {
		Task<T> wrapped = new Task<T>();
		wrapped.options = options;
		wrapped.callable = new Callable<T>() {
			@Override
			public T call() throws Exception {
				try {
					return callable.call();
				} catch (Exception ex) {
					exception.callback(ex);
					return null;
				}
			}
		};
		wrapped.callback = new Callback<T>() {
			@Override
			public void callback(T result) throws Exception {
				try {
					callback.callback(result);
				} catch (Exception ex) {
					exception.callback(ex);
				}
			}
		};
		return wrapped;
	}
}
