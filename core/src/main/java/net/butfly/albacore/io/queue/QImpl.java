package net.butfly.albacore.io.queue;

import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.io.OpenableBasic;

public abstract class QImpl<I, O> extends OpenableBasic implements Q<I, O> {
	private static final long serialVersionUID = -1;
	private final String name;
	private final AtomicLong capacity;

	protected QImpl(String name, long capacity) {
		super();
		this.name = name;
		this.capacity = new AtomicLong(capacity);
	}

	@Override
	public final String name() {
		return name;
	}

	@Override
	public final long capacity() {
		return capacity.get();
	}

	@Override
	public final void open() {
		if (!status.compareAndSet(Status.CLOSED, Status.OPENING)) throw new RuntimeException("Opening failure since status not CLOSED.");
		logger.debug(name() + " opening...");
		opening();
		if (!status.compareAndSet(Status.OPENING, Status.OPENED)) throw new RuntimeException("Opened failure since status not OPENING.");
		logger.info(name() + " opened.");
	}

	@Override
	public final void close() {
		super.close();
		if (!status.compareAndSet(Status.OPENED, Status.CLOSING)) throw new RuntimeException("Closing failure since status not OPENED.");
		logger.debug(name() + " closing...");
		closing();
		if (!status.compareAndSet(Status.CLOSING, Status.CLOSED)) throw new RuntimeException("Closed failure since status not CLOSING.");
		logger.info(name() + " closed.");
	}
}
