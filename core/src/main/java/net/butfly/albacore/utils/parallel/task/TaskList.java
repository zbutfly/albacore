package net.butfly.albacore.utils.parallel.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.utils.parallel.Parals;

public class TaskList implements Task {
	final boolean concurrent;
	final List<Runnable> subs;

	/**
	 * Default sequantial task list.
	 * 
	 * @param first
	 * @param others
	 */
	public TaskList(Runnable first, Runnable then, Runnable... others) {
		this(false, first, then, others);
	}

	public TaskList(boolean concurrent, Runnable first, Runnable then, Runnable... others) {
		super();
		if (null == first || null == then) throw new NullPointerException("TaskList need at least 2 task to run");
		this.concurrent = concurrent;
		subs = new ArrayList<>();
		subs.add(first);
		subs.add(then);
		if (null != others && others.length > 0) subs.addAll(Arrays.asList(others));
	}

	@Override
	public void run() {
		if (concurrent) Parals.run(((TaskList) this).subs.toArray(new Task[0]));
		else for (Runnable t : subs)
			t.run();
	}

	private TaskList compact() {
		int i = 0;
		while (i < subs.size()) {
			Runnable s = subs.get(i);
			if (s instanceof TaskList) {
				TaskList ss = ((TaskList) s).compact();
				if (ss.concurrent == concurrent) {
					subs.remove(i);
					subs.addAll(i, ss.subs);
					i += ss.subs.size();
					continue;
				} else subs.set(i, ss);
			}
			i++;
		}
		return this;
	}

	private TaskList append(Runnable run) {
		subs.add(run);
		return this;
	}

	@Override
	public Task concat(Runnable then) {
		return (concurrent ? new TaskList(false, this, then) : append(then)).compact();
	}

	@Override
	public Task multiple(Runnable other) {
		return (concurrent ? append(other) : new TaskList(true, this, other)).compact();
	}
}
