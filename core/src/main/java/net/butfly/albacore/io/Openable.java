package net.butfly.albacore.io;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import net.butfly.albacore.base.Named;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albacore.utils.parallel.Concurrents;

public interface Openable extends AutoCloseable, Loggable, Named {
	enum Status {
		CLOSED, OPENING, OPENED, CLOSING
	}

	default boolean opened() {
		return Opened.status(this).get() == Status.OPENED;
	}

	default boolean closed() {
		return Opened.status(this).get() == Status.CLOSED;
	}

	default void opening(Runnable handler) {
		Opened.OPENING.computeIfPresent(this, (self, orig) -> () -> {
			orig.run();
			handler.run();
		});
	}

	default void closing(Runnable handler) {
		Opened.CLOSING.computeIfPresent(this, (self, orig) -> () -> {
			orig.run();
			handler.run();
		});
	}

	default void open() {
		AtomicReference<Status> s = Opened.STATUS.computeIfAbsent(this, o -> new AtomicReference<Status>(Status.CLOSED));
		if (s.compareAndSet(Status.CLOSED, Status.OPENING)) {
			logger().trace(name() + " opening...");
			Runnable h = Opened.OPENING.get(this);
			if (null != h) h.run();
			if (!s.compareAndSet(Status.OPENING, Status.OPENED)) //
				throw new RuntimeException("Opened failure since status [" + s.get() + "] not OPENING.");
		}
		if (s.get() != Status.OPENED) //
			throw new RuntimeException("Start failure since status [" + s.get() + "] not OPENED.");
		logger().trace(name() + " opened.");
	}

	@Override
	default void close() {
		AtomicReference<Status> s = Opened.status(this);
		if (s.compareAndSet(Status.OPENED, Status.CLOSING)) {
			logger().trace(name() + " closing...");
			Runnable h = Opened.CLOSING.get(this);
			if (null != h) h.run();
			s.compareAndSet(Status.CLOSING, Status.CLOSED);
		} // else logger().warn(name() + " closing again?");
		while (!closed())
			Concurrents.waitSleep(500, logger(), "Waiting for closing finished...");
		Opened.STATUS.remove(this);
		logger().trace(name() + " closed.");
	}

	class Opened {
		private final static Map<Openable, AtomicReference<Status>> STATUS = new ConcurrentHashMap<>();
		private final static Map<Openable, Runnable> OPENING = new ConcurrentHashMap<>(), CLOSING = new ConcurrentHashMap<>();

		private Opened() {}

		private static AtomicReference<Status> status(Openable inst) {
			return STATUS.getOrDefault(inst, new AtomicReference<>(Status.CLOSED));
		}
	}
}
