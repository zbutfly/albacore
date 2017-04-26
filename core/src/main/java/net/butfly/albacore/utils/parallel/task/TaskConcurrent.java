package net.butfly.albacore.utils.parallel.task;

import net.butfly.albacore.utils.parallel.Parals;

public class TaskConcurrent extends TaskList {
	public TaskConcurrent(Runnable first, Runnable then, Runnable... others) {
		super(first, then, others);
	}

	@Override
	public void run() {
		Parals.run(((TaskConcurrent) this).subs.toArray(new Task[subs.size()]));
	}

	@Override
	public Task multiple(Runnable other) {
		return append(other).clone();
	}

	@Override
	protected final boolean concurrent() {
		return true;
	}
}
