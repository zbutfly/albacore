package net.butfly.albacore.io;

import java.util.concurrent.atomic.AtomicReference;

import net.butfly.albacore.utils.logger.Logger;

public abstract class OpenableBasic implements Openable {
	private static final Logger logger = Logger.getLogger(Openable.class);

	protected final AtomicReference<Status> status;

	protected OpenableBasic() {
		super();
		status = new AtomicReference<>(Status.CLOSED);
		open();
	}

	protected void opening() {}

	protected void closing() {}

	protected boolean isOpen() {
		return status.get() == Status.OPENED;
	}

	@Override
	public void open() {
		if (!status.compareAndSet(Status.CLOSED, Status.OPENING)) throw new RuntimeException("Opening failure since status not CLOSED.");
		logger.debug(getClass().getName() + " opening...");
		opening();
		if (!status.compareAndSet(Status.OPENING, Status.OPENED)) throw new RuntimeException("Opened failure since status not OPENING.");
		logger.info(getClass().getName() + " opened.");
	}

	@Override
	public void close() {
		if (!status.compareAndSet(Status.OPENED, Status.CLOSING)) throw new RuntimeException("Closing failure since status not OPENED.");
		logger.debug(getClass().getName() + " closing...");
		closing();
		if (!status.compareAndSet(Status.CLOSING, Status.CLOSED)) throw new RuntimeException("Closed failure since status not CLOSING.");
		logger.info(getClass().getName() + " closed.");
	}
}
