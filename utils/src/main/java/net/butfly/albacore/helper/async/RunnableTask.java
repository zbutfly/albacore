package net.butfly.albacore.helper.async;

public class RunnableTask extends Thread {
	private static final String THREAD_NAME_PREFIX = "albacore-task-thread-";

	private AsyncTaskBase task;

	public RunnableTask(final AsyncTaskBase task) {
		this.task = task;
		this.setUncaughtExceptionHandler(new AsyncTaskExceptionHandler(task));
		this.setName(THREAD_NAME_PREFIX + task.getName());
	}

	public AsyncTaskBase getTask() {
		return task;
	}

	public void run() {
		Object r = null;
		try {
			r = task.execute();
		} catch (Exception ex) {
			throw new AsyncTaskException(this, ex);
		}
		task.callback(r);
	}
}
