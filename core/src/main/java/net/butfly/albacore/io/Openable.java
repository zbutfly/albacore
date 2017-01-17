package net.butfly.albacore.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import net.butfly.albacore.utils.logger.Logger;

public interface Openable extends AutoCloseable {
	final static Map<Openable, AtomicReference<Status>> STATUS = new ConcurrentHashMap<>();
	final static Logger logger = Logger.getLogger(Openable.class);

	enum Status {
		CLOSED, OPENING, OPENED, CLOSING
	}

	default void opening() {
		logger.debug(name() + " opening...");
	}

	default void closing() {
		logger.debug(name() + " closing...");
	}

	default void open() {
		AtomicReference<Status> s = STATUS.computeIfAbsent(this, o -> new AtomicReference<Status>(Status.CLOSED));
		if (s.compareAndSet(Status.CLOSED, Status.OPENING)) {
			opening();
			if (!s.compareAndSet(Status.OPENING, Status.OPENED)) //
				throw new RuntimeException("Opened failure since status [" + s.get() + "] not OPENING.");
			logger.debug(name() + " opened.");
		}
		if (s.get() != Status.OPENED) //
			throw new RuntimeException("Start failure since status [" + s.get() + "] not OPENED.");
	}

	@Override
	default void close() {
		if (status().compareAndSet(Status.OPENED, Status.CLOSING)) {
			closing();
			if (!status().compareAndSet(Status.CLOSING, Status.CLOSED))//
				throw new RuntimeException("Closed failure since status [" + status().get() + "] not CLOSING.");
			logger.debug(name() + " closed.");
		}
		if (status().get() != Status.CLOSED) //
			throw new RuntimeException("Closing failure since status [" + status().get() + "] not OPENED.");
		else STATUS.remove(this);
	}

	default void close(Runnable closing) {
		if (status().compareAndSet(Status.OPENED, Status.CLOSING)) {
			logger.debug(name() + " closing...");
			closing.run();
			if (!status().compareAndSet(Status.CLOSING, Status.CLOSED))//
				throw new RuntimeException("Closed failure since status [" + status().get() + "] not CLOSING.");
			logger.debug(name() + " closed.");
		}
		if (status().get() != Status.CLOSED) //
			throw new RuntimeException("Closing failure since status [" + status().get() + "] not OPENED.");
		else STATUS.remove(this);
	}

	default AtomicReference<Status> status() {
		return STATUS.getOrDefault(this, new AtomicReference<>(Status.CLOSED));
	}

	default String name() {
		return getClass().getSimpleName();
	}

	default boolean opened() {
		return status().get() == Status.OPENED;
	}

	default boolean closed() {
		return status().get() == Status.CLOSED;
	}
}
