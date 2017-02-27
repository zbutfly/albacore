package net.butfly.albacore.io;

import java.util.concurrent.atomic.AtomicBoolean;

import net.butfly.albacore.utils.parallel.Concurrents;

public class OpenableThread extends Thread implements Openable {
	private final AtomicBoolean started = new AtomicBoolean(false);

	private static final ThreadGroup g = new ThreadGroup("OpenableThreads");

	public OpenableThread(String name) {
		super(g, name);
	}

	public OpenableThread(Runnable target) {
		super(g, target);
	}

	public OpenableThread(Runnable target, String name) {
		super(g, target, name);
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
	public void open(Runnable run) {
		setUncaughtExceptionHandler((t, e) -> logger().error(getName() + " failure", e));
		Openable.super.open(run);
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
