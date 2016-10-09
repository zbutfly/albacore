package net.butfly.albacore.io.impl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.event.Level;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;

public class LoggerOutput extends OutputQueueImpl<String, String> implements OutputQueue<String> {
	private static final long serialVersionUID = 7782039002400807964L;
	private final Map<Level, Consumer<String>> loggings;
	private final Logger logger;
	private final Level level;

	public LoggerOutput() {
		this(Thread.currentThread().getStackTrace()[2].getClassName(), Level.INFO);
	}

	public LoggerOutput(Class<?> clazz) {
		this(clazz.getName(), Level.INFO);
	}

	public LoggerOutput(Level level) {
		this(Thread.currentThread().getStackTrace()[2].getClassName(), level);
	}

	public LoggerOutput(Class<?> clazz, Level level) {
		this(clazz.getName(), level);
		StackTraceElement s = Thread.currentThread().getStackTrace()[2];
		s.getClassName();
		s.getMethodName();
		s.getLineNumber();
	}

	private LoggerOutput(String loggerName, Level level) {
		super("LOGGER-OUTPUT-QUEUE-" + level.name());
		this.level = level;
		this.logger = Instances.fetch(() -> Logger.getLogger(loggerName), Logger.class, loggerName);
		loggings = new HashMap<>();
		if (loggings.isEmpty()) {
			loggings.put(Level.TRACE, s -> logger.trace(s));
			loggings.put(Level.DEBUG, s -> logger.debug(s));
			loggings.put(Level.INFO, s -> logger.info(s));
			loggings.put(Level.WARN, s -> logger.warn(s));
			loggings.put(Level.ERROR, s -> logger.error(s));
		}
	}

	@Override
	protected boolean enqueueRaw(String message) {
		stats(Act.INPUT, message);
		loggings.get(level).accept(message);
		return true;
	}
}
