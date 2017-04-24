package net.butfly.albacore.utils.parallel.task;

import net.butfly.albacore.utils.parallel.Parals;

public interface Task extends Runnable {
	@Override
	default void run() {
		if (this instanceof SinTask) ((SinTask) this).running.run();
		else if (this instanceof ListTask) {
			if (((ListTask) this).concurrent) Parals.run(((ListTask) this).subs.toArray(new Task[0]));
			else for (Runnable t : ((ListTask) this).subs)
				t.run();
		}
	}

	default Task concat(Runnable then) {
		if (this instanceof SinTask || ((ListTask) this).concurrent) return new ListTask(false, this, then);
		else {
			((ListTask) this).subs.add(then);
			return this;
		}
	}

	default Task multiple(Runnable other) {
		if (this instanceof ListTask && ((ListTask) this).concurrent) {
			((ListTask) this).subs.add(other);
			return this;
		} else return new ListTask(true, this, other);
	}

	public static void main(String... args) {
		new SinTask(() -> test("select * from edw_gazhk_czrk limit 10"))//
				.multiple(//
						new SinTask(() -> test("select * from sfqh where code='11'"))//
								.concat(() -> test("select * from sfqh where code='14'"))//
								.concat(() -> test("select * from sfqh where code='52'"))//
				)//
				.concat(() -> test("select * from edw_gazhk_czrk_par limit 10;"))//
				.run();
	}

	static void test(String sql) {
		System.out.println(Thread.currentThread().getName() + "[" + Thread.currentThread().getId() + "]: " + sql);
	}
}
