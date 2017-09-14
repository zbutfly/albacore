package net.butfly.albacore.utils;

import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.utils.parallel.Concurrents;

public class OpenableThread extends Thread implements Openable {
	// Thread status: 0: not started, 1: running, 2: stopped
	private final AtomicInteger runned = new AtomicInteger(0);

	private static final ThreadGroup g = new ThreadGroup("OpenableThreads");

	public OpenableThread(String name) {
		super(g, name);
		init();
	}

	private void init() {
		setUncaughtExceptionHandler((t, e) -> logger().error(getName() + " failure", e));
		opening(super::start);
	}

	public OpenableThread(Runnable target) {
		super(g, target);
		init();
	}

	public OpenableThread(Runnable target, String name) {
		super(g, target, name);
		init();
	}

	public OpenableThread(ThreadGroup group, Runnable target, String name) {
		super(group, target, name);
		init();
	}

	public OpenableThread(ThreadGroup group, Runnable target) {
		super(group, target);
		init();
	}

	public OpenableThread(ThreadGroup group, String name) {
		super(group, name);
		init();
	}

	@Override
	public final void run() {
		runned.set(1);
		try {
			exec();
		} finally {
			logger().debug(name() + " close.");
			runned.set(2);
		}
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
		Openable.super.open();
		while (runned.get() <= 0)
			Concurrents.waitSleep(10);
	}

	@Override
	public void start() {
		open();
	}

	@Override
	public void close() {
		Openable.super.close();
		while (runned.get() < 2)
			Concurrents.waitSleep(100);
	}
}
