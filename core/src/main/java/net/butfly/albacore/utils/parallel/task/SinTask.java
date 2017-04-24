package net.butfly.albacore.utils.parallel.task;

public class SinTask implements Task {
	final Runnable running;

	public SinTask(Runnable running) {
		super();
		this.running = running;
	}
}