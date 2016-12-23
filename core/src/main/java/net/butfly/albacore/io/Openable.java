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

	default void opening() {}

	default void closing() {}

	default void open() {
		if (status().compareAndSet(Status.CLOSED, Status.OPENING)) {
			logger.debug(name() + " opening...");
			opening();
			if (!status().compareAndSet(Status.OPENING, Status.OPENED)) //
				throw new RuntimeException("Opened failure since status [" + status().get() + "] not OPENING.");
			logger.debug(name() + " opened.");
		} ;
		if (status().get() != Status.OPENED) //
			throw new RuntimeException("Start failure since status [" + status().get() + "] not OPENED.");
	}

	default void close() {
		if (status().compareAndSet(Status.OPENED, Status.CLOSING)) {
			logger.debug(name() + " closing...");
			closing();
			if (!status().compareAndSet(Status.CLOSING, Status.CLOSED))//
				throw new RuntimeException("Closed failure since status [" + status().get() + "] not CLOSING.");
			logger.debug(name() + " closed.");
		}
		if (status().get() != Status.CLOSED) //
			throw new RuntimeException("Closing failure since status [" + status().get() + "] not OPENED.");
		else STATUS.remove(this);
	}

	default AtomicReference<Status> status() {
		return STATUS.computeIfAbsent(this, o -> new AtomicReference<Status>(Status.CLOSED));
	}

	default String name() {
		return getClass().getSimpleName();
	}

	default boolean opened() {
		return status().get() == Status.OPENED;
	}
}
