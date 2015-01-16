package net.butfly.albacore.utils.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import net.butfly.albacore.utils.Exceptions;

public class Task<T> {
	protected Callable<T> call;
	protected Callback<T> back;
	protected Options options;

	public static class ExceptionHandler<R> {
		public R handle(final Exception exception) throws Exception {
			throw Exceptions.unwrap(exception);
		}
	}

	public abstract static class Callback<R> extends Tasks.Handleable<R> {
		public abstract void callback(final R result) throws Exception;
	}

	public abstract static class Callable<R> extends Tasks.Handleable<R> implements java.util.concurrent.Callable<R> {
		public abstract R call() throws Exception;
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
		this.call = task;
		this.back = callback;
		this.options = options;
	}

	public Callable<T> call() {
		return call;
	}

	public Callback<T> back() {
		return back;
	}

	public Options options() {
		return options;
	}

	public enum HandlerTarget {
		CALLABLE, CALLBACK
	}

	public Task<T> exception(Task.ExceptionHandler<T> handler, HandlerTarget... targets) {
		return Tasks.wrapHandler(this, handler, targets);
	}

	public T execute() throws Exception {
		return this.execute(null);
	}

	public T execute(ExecutorService executor) throws Exception {
		return Tasks.execute(this, executor);
	}

	public static Executor getDefaultExecutor() {
		return Tasks.EXECUTOR;
	}
}
