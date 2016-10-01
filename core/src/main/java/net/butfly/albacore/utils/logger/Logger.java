package net.butfly.albacore.utils.logger;

import java.io.Serializable;

import net.butfly.albacore.lambda.Supplier;

public class Logger implements Serializable {
	private static final long serialVersionUID = -1940330974751419775L;
	private final org.slf4j.Logger logger;

	private Logger(org.slf4j.Logger logger) {
		super();
		this.logger = logger;
	}

	public static final Logger getLogger(String name) {
		return new Logger(org.slf4j.LoggerFactory.getLogger(name));
	}

	public static final Logger getLogger(Class<?> clazz) {
		return new Logger(org.slf4j.LoggerFactory.getLogger(clazz));
	}

	public Logger(String name) {
		this(org.slf4j.LoggerFactory.getLogger(name));
	}

	public Logger(Class<?> clazz) {
		this(org.slf4j.LoggerFactory.getLogger(clazz));
	}

	public String getName() {
		return logger.getName();
	}

	public boolean isTraceEnabled() {
		return logger.isTraceEnabled();
	}

	public boolean trace(String msg) {
		logger.trace(msg);
		return true;
	}

	public boolean trace(Supplier<String> msg) {
		if (logger.isTraceEnabled()) {
			String m = msg.get();
			if (null != m) logger.trace(m);
		}
		return true;
	}

	public boolean isDebugEnabled() {
		return logger.isDebugEnabled();
	}

	public boolean debug(String msg) {
		logger.debug(msg);
		return true;
	}

	public boolean debug(Supplier<String> msg) {
		if (logger.isDebugEnabled()) {
			String m = msg.get();
			if (null != m) logger.debug(m);
		}
		return true;
	}

	public boolean isInfoEnabled() {
		return logger.isInfoEnabled();
	}

	public boolean info(String msg) {
		logger.info(msg);
		return true;
	}

	public boolean info(Supplier<String> msg) {
		if (logger.isInfoEnabled()) {
			String m = msg.get();
			if (null != m) logger.info(m);
		}
		return true;
	}

	public boolean isWarnEnabled() {
		return logger.isWarnEnabled();
	}

	public boolean warn(String msg) {
		logger.warn(msg);
		return true;
	}

	public boolean warn(Supplier<String> msg) {
		if (logger.isWarnEnabled()) {
			String m = msg.get();
			if (null != m) logger.warn(m);
		}
		return true;
	}

	public boolean isErrorEnabled() {
		return logger.isErrorEnabled();
	}

	public boolean error(String msg) {
		logger.error(msg);
		return true;
	}

	public boolean error(Supplier<String> msg) {
		if (logger.isErrorEnabled()) {
			String m = msg.get();
			if (null != m) logger.error(m);
		}
		return true;
	}

	public boolean trace(Supplier<String> msg, Throwable t) {
		if (logger.isTraceEnabled()) {
			String m = msg.get();
			if (null != m) logger.trace(m, t);
		}
		return true;
	}

	public boolean debug(Supplier<String> msg, Throwable t) {
		if (logger.isDebugEnabled()) {
			String m = msg.get();
			if (null != m) logger.debug(m, t);
		}
		return true;
	}

	public boolean info(Supplier<String> msg, Throwable t) {
		if (logger.isInfoEnabled()) {
			String m = msg.get();
			if (null != m) logger.info(m, t);
		}
		return true;
	}

	public boolean warn(Supplier<String> msg, Throwable t) {
		if (logger.isWarnEnabled()) {
			String m = msg.get();
			if (null != m) logger.warn(m, t);
		}
		return true;
	}

	public boolean error(Supplier<String> msg, Throwable t) {
		if (logger.isErrorEnabled()) {
			String m = msg.get();
			if (null != m) logger.error(m, t);
		}
		return true;
	}

	public boolean trace(String msg, Throwable t) {
		logger.trace(msg, t);
		return true;
	}

	public boolean debug(String msg, Throwable t) {
		logger.debug(msg, t);
		return true;
	}

	public boolean info(String msg, Throwable t) {
		logger.info(msg, t);
		return true;
	}

	public boolean warn(String msg, Throwable t) {
		logger.warn(msg, t);
		return true;
	}

	public boolean error(String msg, Throwable t) {
		logger.error(msg, t);
		return true;
	}

	/** Old style */
	public boolean trace(String format, Object arg) {
		logger.trace(format, arg);
		return true;
	}

	public boolean trace(String format, Object arg1, Object arg2) {
		logger.trace(format, arg1, arg2);
		return true;
	}

	public boolean trace(String format, Object... arguments) {
		logger.trace(format, arguments);
		return true;
	}

	public boolean debug(String format, Object arg) {
		logger.debug(format, arg);
		return true;
	}

	public boolean debug(String format, Object arg1, Object arg2) {
		logger.debug(format, arg1, arg2);
		return true;
	}

	public boolean debug(String format, Object... arguments) {
		logger.debug(format, arguments);
		return true;
	}

	public boolean info(String format, Object arg) {
		logger.info(format, arg);
		return true;
	}

	public boolean info(String format, Object arg1, Object arg2) {
		logger.info(format, arg1, arg2);
		return true;
	}

	public boolean info(String format, Object... arguments) {
		logger.info(format, arguments);
		return true;
	}

	public boolean warn(String format, Object arg) {
		logger.warn(format, arg);
		return true;
	}

	public boolean warn(String format, Object... arguments) {
		logger.warn(format, arguments);
		return true;
	}

	public boolean warn(String format, Object arg1, Object arg2) {
		logger.warn(format, arg1, arg2);
		return true;
	}

	public boolean error(String format, Object arg) {
		logger.error(format, arg);
		return true;
	}

	public boolean error(String format, Object arg1, Object arg2) {
		logger.error(format, arg1, arg2);
		return true;
	}

	public boolean error(String format, Object... arguments) {
		logger.error(format, arguments);
		return true;
	}

	/** Ignore, we will never use Marker */
	// public boolean isTraceEnabled(Marker marker)
	// public void trace(Marker marker, String msg)
	// public void trace(Marker marker, String format, Object arg)
	// public void trace(Marker marker, String format, Object arg1, Object arg2)
	// public void trace(Marker marker, String format, Object... argArray)
	// public void trace(Marker marker, String msg, Throwable t)
	// public boolean isDebugEnabled(Marker marker)
	// public void debug(Marker marker, String msg)
	// public void debug(Marker marker, String format, Object arg)
	// public void debug(Marker marker, String format, Object arg1, Object arg2)
	// public void debug(Marker marker, String format, Object... arguments)
	// public void debug(Marker marker, String msg, Throwable t)
	// public boolean isInfoEnabled(Marker marker)
	// public void info(Marker marker, String msg)
	// public void info(Marker marker, String format, Object arg)
	// public void info(Marker marker, String format, Object arg1, Object arg2)
	// public void info(Marker marker, String format, Object... arguments)
	// public void info(Marker marker, String msg, Throwable t)
	// public boolean isWarnEnabled(Marker marker)
	// public void warn(Marker marker, String msg)
	// public void warn(Marker marker, String format, Object arg)
	// public void warn(Marker marker, String format, Object arg1, Object arg2)
	// public void warn(Marker marker, String format, Object... arguments)
	// public void warn(Marker marker, String msg, Throwable t)
	// public boolean isErrorEnabled(Marker marker)
	// public void error(Marker marker, String msg)
	// public void error(Marker marker, String format, Object arg)
	// public void error(Marker marker, String format, Object arg1, Object arg2)
	// public void error(Marker marker, String format, Object... arguments)
	// public void error(Marker marker, String msg, Throwable t)
}