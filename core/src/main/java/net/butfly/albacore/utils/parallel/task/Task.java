package net.butfly.albacore.utils.parallel.task;

public interface Task extends Runnable {
	static Task task(Runnable r) {
		return r instanceof Task ? (Task) r : r::run;
	}

	default Task concat(Runnable then) {
		return new TaskList(this, then);
	}

	default Task multiple(Runnable other) {
		return new TaskList(true, this, other);
	}

	public static void main(String... args) {
		task(() -> test("select * from edw_gazhk_czrk limit 10"))//
				.multiple(//
						task(() -> test("select * from sfqh where code='11'"))//
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
