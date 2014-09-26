package net.butfly.albacore.utils;

import java.util.concurrent.Callable;

public abstract class AsyncTask<OUT> implements Callable<OUT> {
	private AsyncCallback<OUT> callback;

	public AsyncTask(AsyncCallback<OUT> callback) {
		super();
		this.callback = callback;
	}

	public final AsyncCallback<OUT> getCallback() {
		return this.callback;
	}

	public static interface AsyncCallback<T> {
		void callback(T result);
	}
}
