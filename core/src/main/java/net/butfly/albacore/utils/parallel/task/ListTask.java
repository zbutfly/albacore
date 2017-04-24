package net.butfly.albacore.utils.parallel.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListTask implements Task {
	final boolean concurrent;
	final List<Runnable> subs;

	public ListTask(boolean concurrent, Runnable first, Runnable... others) {
		super();
		this.concurrent = concurrent;
		subs = new ArrayList<>();
		subs.add(first);
		subs.addAll(Arrays.asList(others));
	}
}
