package net.butfly.albacore.utils.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import net.butfly.albacore.lambda.Callable;
import net.butfly.albacore.lambda.Consumer;

public class Task<T> {
	protected Callable<T> call;
	protected Consumer<T> back;
	protected Options options;
	protected ExceptionHandler<T> handler = null;

	@FunctionalInterface
	public interface ExceptionHandler<R> {
		R handle(final Exception exception) throws Exception;
	}

	protected Task() {}

	public Task(Callable<T> task) {
		this(task, null, null);
	}

	public Task(Callable<T> task, Consumer<T> callback) {
		this(task, callback, null);
	}

	public Task(Callable<T> task, Options options) {
		this(task, null, options);
	}

	public Task(Callable<T> task, Consumer<T> callback, Options options) {
		this.call = task;
		this.back = callback;
		this.options = options;
	}

	public Callable<T> call() {
		return call;
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
		return this.execute(Tasks.CORE_EXECUTOR);
	}

	public T execute(ExecutorService executor) throws Exception {
		return Tasks.execute(this, executor);
	}

	public static Executor getDefaultExecutor() {
		return Tasks.CORE_EXECUTOR;
	}
}
