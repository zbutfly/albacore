package net.butfly.albacore.helper.async;

import net.butfly.albacore.exception.SystemException;

public class AsyncTaskException extends SystemException {

	private static final long serialVersionUID = 5644223873338641001L;

	public AsyncTaskException(RunnableTask runnableTask, Throwable cause) {
		super("SYS_099", "Exception in task with name [" + runnableTask.getTask().getName() + "], the thread is: ["
				+ runnableTask.getName() + "](" + runnableTask.getId() + ").", cause);
	}
}
