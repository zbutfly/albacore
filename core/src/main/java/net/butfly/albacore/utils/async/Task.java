package net.butfly.albacore.utils.async;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import net.butfly.albacore.utils.parallel.Concurrents;

public class Task<T> {
	protected java.util.concurrent.Callable<T> call;
	protected Consumer<T> back;
	protected Options options;
	protected ExceptionHandler<T> handler = null;

	@FunctionalInterface
	public interface ExceptionHandler<R> {
		R handle(final Exception exception) throws Exception;
	}

	protected Task() {}

	public Task(java.util.concurrent.Callable<T> task) {
		this(task, null, null);
	}

	public Task(java.util.concurrent.Callable<T> task, Consumer<T> callback) {
		this(task, callback, null);
	}

	public Task(java.util.concurrent.Callable<T> task, Options options) {
		this(task, null, options);
	}

	public Task(java.util.concurrent.Callable<T> task, Consumer<T> callback, Options options) {
		this.call = task;
		this.back = callback;
		this.options = options;
	}

	public Callable<T> call() {
		if (call instanceof Callable) return (Callable<T>) call;
		else return new Callable<T>() {
			@Override
			public T call() throws Exception {
				try {
					return call.call();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	public Consumer<T> back() {
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
		return this.execute(Concurrents.executor());
	}

	public T execute(ExecutorService executor) throws Exception {
		return Tasks.execute(this, executor);
	}

	public static Executor getDefaultExecutor() {
		return Concurrents.executor();
	}
}
