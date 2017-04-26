package net.butfly.albacore.utils.parallel.task;

public class TaskConsecutive extends TaskList {
	public TaskConsecutive(Runnable first, Runnable then, Runnable... others) {
		super(first, then, others);
	}

	@Override
	public void run() {
		for (Runnable t : subs)
			t.run();
	}

	@Override
	public Task concat(Runnable then) {
		return append(then).clone();
	}

	@Override
	protected final boolean concurrent() {
		return false;
	}
}
