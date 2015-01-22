package net.butfly.albacore.helper.async;

public final class AsyncTaskExceptionHandler extends ThreadGroup {

	private AsyncTaskBase task;

	public AsyncTaskExceptionHandler(AsyncTaskBase task) {
		super("Asynchronous call thread group");
		this.task = task;
	}

	public void uncaughtException(Thread t, Throwable e) {
		try {
			task.exception(t, e);
		} catch (Exception ex) {
			super.uncaughtException(t, ex);
		}
	}
}
