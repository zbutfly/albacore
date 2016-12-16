package net.butfly.albacore.io;

import java.util.concurrent.atomic.AtomicReference;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public abstract class OpenableThread extends Thread implements Openable {
	private static final Logger logger = Logger.getLogger(OpenableThread.class);

	protected final AtomicReference<Status> status;

	private AtomicReference<Status> initme() {
		setUncaughtExceptionHandler((t, e) -> logger.error(getName() + " failure", e));
		return new AtomicReference<>(Status.CLOSED);
	}

	public OpenableThread() {
		super();
		status = initme();
	}

	public OpenableThread(String name) {
		super(name);
		status = initme();
	}

	public OpenableThread(Runnable target) {
		super(target);
		status = initme();
	}

	public OpenableThread(Runnable target, String name) {
		super(target, name);
		status = initme();
	}

	@Override
	public void start() {
		if (status.compareAndSet(Status.CLOSED, Status.OPENING)) go();
		if (status.get() != Status.OPENED) throw new RuntimeException("Start failure since status not OPENED.");
	}

	protected void initialize() {}

	protected void closing() {}

	protected boolean opened() {
		return status.get() == Status.OPENED;
	}

	@Override
	public void open() {
		if (!status.compareAndSet(Status.CLOSED, Status.OPENING)) throw new RuntimeException("Opening failure since status not CLOSED.");
		go();
	}

	private void go() {
		logger.debug(getName() + " opening...");
		initialize();
		if (!status.compareAndSet(Status.OPENING, Status.OPENED)) throw new RuntimeException("Opened failure since status not OPENING.");
		logger.info(getName() + " opened.");
		super.start();
		logger.info(getName() + " started.");
	}

	@Override
	public void close() {
		if (!status.compareAndSet(Status.OPENED, Status.CLOSING)) throw new RuntimeException("Closing failure since status not OPENED.");
		logger.debug(getClass().getName() + " closing...");
		closing();
		while (this.isAlive() && !this.isInterrupted())
			Concurrents.waitSleep(100);
		if (!status.compareAndSet(Status.CLOSING, Status.CLOSED)) throw new RuntimeException("Closed failure since status not CLOSING.");
		logger.info(getClass().getName() + " closed.");
	}

	public Status status() {
		return status.get();
	}
}
