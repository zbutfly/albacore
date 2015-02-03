package net.butfly.albacore.utils.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class Task<T> {
	protected Callable<T> call;
	protected Callback<T> back;
	protected Options options;
	protected ExceptionHandler<T> handler = null;

	public interface ExceptionHandler<R> {
		R handle(final Exception exception) throws Exception;
	}

	public interface Callback<R> {
		public void callback(final R result);
	}

	public interface Callable<R> extends java.util.concurrent.Callable<R> {
		public R call() throws Exception;
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

	public Task<T> handler(Task.ExceptionHandler<T> handler) {
		this.handler = handler;
		return this;
	}

	public ExceptionHandler<T> handler() {
		return this.handler;
	}

	public T execute() throws Exception {
		return this.execute(Tasks.EXECUTOR);
	}

	public T execute(ExecutorService executor) throws Exception {
		return Tasks.execute(this, executor);
	}

	public static Executor getDefaultExecutor() {
		return Tasks.EXECUTOR;
	}
}
