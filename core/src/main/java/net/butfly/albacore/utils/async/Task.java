package net.butfly.albacore.utils.async;

import java.util.concurrent.Callable;

public class Task<T> {
	protected Callable<T> task;
	protected Callback<T> callback;
	protected Options options;

	protected Task() {}

	public Task(Callable<T> task) {
		this(task, null, new Options());
	}

	public Task(Callable<T> task, Callback<T> callback) {
		this(task, callback, new Options());
	}

	public Task(Callable<T> task, Options options) {
		this(task, null, options);
	}

	public Task(Callable<T> task, Callback<T> callback, Options options) {
		this.task = task;
		this.callback = callback;
		this.options = options;
	}

	public Callable<T> task() {
		return task;
	}

	public Callback<T> callback() {
		return callback;
	}

	public Options options() {
		return options;
	}
}
