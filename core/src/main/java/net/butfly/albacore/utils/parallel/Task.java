package net.butfly.albacore.utils.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface Task extends Runnable {
	static Task task(Runnable task) {
		return task::run;
	}

	default Task concat(Runnable then) {
		return new Tasks.TaskConsecutive(this, then).compact();
	}

	default Task multiple(Runnable other) {
		return new Tasks.TaskConcurrent(this, other).compact();
	}

	default Task unconcurrent() {
		return this;
	}

	default Task async() {
		return () -> Parals.listenRun(this::run);
	}

	default String text() {
		String[] segs = toString().split("/");
		return "Task[" + (segs.length == 1 ? segs[0] : segs[segs.length - 1]) + "]";
	}

	final class Tasks {
		private static abstract class TaskList implements Task {
			protected final List<Runnable> subs;

			protected abstract boolean concurrent();

			private TaskList(Runnable first, Runnable then, Runnable... others) {
				super();
				if (null == first || null == then) throw new NullPointerException("TaskList need at least 2 task to run");
				subs = new ArrayList<>();
				subs.add(first);
				subs.add(then);
				if (null != others && others.length > 0) subs.addAll(Arrays.asList(others));
			}

			@Override
			public Task unconcurrent() {
				Runnable[] nsubs = subs.toArray(new Runnable[subs.size()]);
				for (int i = 0; i < nsubs.length; i++)
					if (nsubs[i] instanceof TaskList) nsubs[i] = ((TaskList) nsubs[i]).unconcurrent();
				return new TaskConsecutive(nsubs[0], nsubs[1], Arrays.copyOfRange(nsubs, 2, nsubs.length));
			}

			public final TaskList compact() {
				List<Runnable> nsubs = new ArrayList<>();
				for (Runnable s : subs) {
					if (s instanceof TaskList) {
						TaskList ss = ((TaskList) s).compact();
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

		private static final class TaskConsecutive extends TaskList {
			private TaskConsecutive(Runnable first, Runnable then, Runnable... others) {
				super(first, then, others);
			}

			@Override
			public void run() {
				for (Runnable t : subs)
					t.run();
			}

			@Override
			public Task concat(Runnable then) {
				return append(then).compact();
			}

			@Override
			protected final boolean concurrent() {
				return false;
			}

			@Override
			public String toString() {
				return subs.stream().map(r -> "Task[" + r.toString() + "]").collect(Collectors.joining("+"));
			}
		}

		private static final class TaskConcurrent extends TaskList {
			private TaskConcurrent(Runnable first, Runnable then, Runnable... others) {
				super(first, then, others);
			}

			@Override
			public void run() {
				Parals.run(subs.toArray(new Task[subs.size()]));
			}

			@Override
			public Task multiple(Runnable other) {
				return append(other).compact();
			}

			@Override
			protected final boolean concurrent() {
				return true;
			}

			@Override
			public String toString() {
				return subs.stream().map(r -> "Task[" + r.toString() + "]").collect(Collectors.joining("*"));
			}
		}
	}

	public static void main(String... args) {
		Task t1 = () -> test("select * from edw_gazhk_czrk limit 10");
		Task t21 = () -> test("select * from sfqh where code='11'");
		Task t22 = () -> test("select * from sfqh where code='14'");
		Task t23 = () -> test("select * from sfqh where code='52'");
		Task t2 = t21.multiple(t22).multiple(t23);
		Task t3 = () -> test("select * from edw_gazhk_czrk_par limit 10;");
		Task t = t1.concat(t2).concat(t3);
		t.run();
	}

	static void test(String sql) {
		System.out.println(Thread.currentThread().getName() + "[" + Thread.currentThread().getId() + "]: " + sql);
	}
}
