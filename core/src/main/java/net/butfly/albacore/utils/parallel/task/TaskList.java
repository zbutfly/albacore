package net.butfly.albacore.utils.parallel.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

abstract class TaskList implements Task {
	protected final List<Runnable> subs;

	protected abstract boolean concurrent();

	public TaskList(Runnable first, Runnable then, Runnable... others) {
		super();
		if (null == first || null == then) throw new NullPointerException("TaskList need at least 2 task to run");
		subs = new ArrayList<>();
		subs.add(first);
		subs.add(then);
		if (null != others && others.length > 0) subs.addAll(Arrays.asList(others));
	}

	@Override
	public final TaskList clone() {
		List<Runnable> nsubs = new ArrayList<>();
		for (Runnable s : subs) {
			if (s instanceof TaskList) {
				TaskList ss = ((TaskList) s).clone();
				if (ss.concurrent() == concurrent()) {
					nsubs.addAll(ss.subs);
					continue;
				}
			}
			nsubs.add(s);
		}
		return concurrent() ? new TaskConcurrent(nsubs.remove(0), nsubs.remove(0), nsubs.toArray(new Runnable[nsubs.size()]))
				: new TaskConsecutive(nsubs.remove(0), nsubs.remove(0), nsubs.toArray(new Runnable[nsubs.size()]));
	}

	protected final TaskList append(Runnable run) {
		subs.add(run);
		return this;
	}
}
