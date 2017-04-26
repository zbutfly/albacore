package net.butfly.albacore.utils.parallel.task;

public interface Task extends Runnable, Cloneable {
	static Task task(Runnable r) {
		return r instanceof Task ? (Task) r : r::run;
	}

	default Task concat(Runnable then) {
		return new TaskConsecutive(this, then).clone();
	}

	default Task multiple(Runnable other) {
		return new TaskConcurrent(this, other).clone();
	}

	default Task clone() {
		return this::run;
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
