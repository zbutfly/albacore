package net.butfly.albacore.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Loggable;

public interface Openable extends AutoCloseable, Loggable, Named {

	enum Status {
		CLOSED, OPENING, OPENED, CLOSING
	}

	default void open() {
		open(null);
	}

	default void open(Runnable opening) {
		AtomicReference<Status> s = Opened.STATUS.computeIfAbsent(this, o -> new AtomicReference<Status>(Status.CLOSED));
		if (s.compareAndSet(Status.CLOSED, Status.OPENING)) {
			logger().trace(name() + " opening...");
			if (null != opening) opening.run();
			if (!s.compareAndSet(Status.OPENING, Status.OPENED)) //
				throw new RuntimeException("Opened failure since status [" + s.get() + "] not OPENING.");
		}
		if (s.get() != Status.OPENED) //
			throw new RuntimeException("Start failure since status [" + s.get() + "] not OPENED.");
		logger().trace(name() + " opened.");
	}

	@Override
	default void close() {
		close(null);
	}

	default void close(Runnable closing) {
		AtomicReference<Status> s = Opened.status(this);
		if (s.compareAndSet(Status.OPENED, Status.CLOSING)) {
			logger().trace(name() + " closing...");
			if (null != closing) closing.run();
			s.compareAndSet(Status.CLOSING, Status.CLOSED);
		} // else logger().warn(name() + " closing again?");
		while (!closed())
			Concurrents.waitSleep(500, logger(), "Waiting for closing finished...");
		Opened.STATUS.remove(this);
		logger().trace(name() + " closed.");
	}

	default Status status() {
		return Opened.status(this).get();
	}

	default boolean opened() {
		return status() == Status.OPENED;
	}

	default boolean closed() {
		return status() == Status.CLOSED;
	}

	class Opened {
		private final static Map<Openable, AtomicReference<Status>> STATUS = new ConcurrentHashMap<>();

		private static AtomicReference<Status> status(Openable inst) {
			return Opened.STATUS.getOrDefault(inst, new AtomicReference<>(Status.CLOSED));
		}
	}
}
