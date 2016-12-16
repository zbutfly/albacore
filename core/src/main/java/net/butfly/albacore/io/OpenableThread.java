package net.butfly.albacore.io;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public abstract class OpenableThread extends Thread implements Openable {
	private static final Logger logger = Logger.getLogger(OpenableThread.class);

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

	@Override
	public final String name() {
		return getName();
	}

	@Override
	public void opening() {
		setUncaughtExceptionHandler((t, e) -> logger.error(getName() + " failure", e));
		super.start();
		logger.info(name() + " started.");
	}

	@Override
	public void closing() {
		while (this.isAlive() && !this.isInterrupted())
			Concurrents.waitSleep(100);
	}

	@Override
	public void start() {
		open();
	}
}
