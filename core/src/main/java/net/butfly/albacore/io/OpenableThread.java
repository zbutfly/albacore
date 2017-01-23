package net.butfly.albacore.io;

import java.util.concurrent.atomic.AtomicBoolean;

import net.butfly.albacore.utils.async.Concurrents;

public class OpenableThread extends Thread implements Openable {
	private final AtomicBoolean started = new AtomicBoolean(false);

	public OpenableThread() {
		super();
	}

	public OpenableThread(String name) {
		super(name);
	}

	public OpenableThread(Runnable target) {
		super(target);
	}

	public OpenableThread(Runnable target, String name) {
		super(target, name);
	}

	public OpenableThread(ThreadGroup group, Runnable target, String name) {
		super(group, target, name);
	}

	public OpenableThread(ThreadGroup group, Runnable target) {
		super(group, target);
	}

	public OpenableThread(ThreadGroup group, String name) {
		super(group, name);
	}

	@Override
	public final void run() {
		started.set(true);
		exec();
	}

	protected void exec() {
		super.run();
	}

	@Override
	public final String name() {
		return getName();
	}

	@Override
	public void open() {
		setUncaughtExceptionHandler((t, e) -> logger().error(getName() + " failure", e));
		Openable.super.open();
		super.start();
		while (!started.get())
			Concurrents.waitSleep(100);
	}

	@Override
	public void start() {
		open();
	}

	@Override
	public void close() {
		Openable.super.close();
		while (isAlive() && !isInterrupted())
			Concurrents.waitSleep(100);
	}
}
