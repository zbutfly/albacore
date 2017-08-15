package net.butfly.albacore.utils.logger;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import net.butfly.albacore.Albacore;

public class Logger implements Serializable {
	private static final long serialVersionUID = -1940330974751419775L;
	private static final boolean async;
	private static final AtomicInteger tn;
	private static final ThreadGroup g;
	private static final ExecutorService logex;
	static {
		async = Boolean.parseBoolean(System.getProperty(Albacore.Props.PROP_LOGGER_ASYNC, "true"));
		if (async) {
			tn = new AtomicInteger();
			g = new ThreadGroup("AlbacoreLoggerThread");
			logex = Executors.newCachedThreadPool(r -> {
				Thread t = new Thread(g, r, "AlbacoreLoggerThread#" + tn.getAndIncrement());
				t.setDaemon(true);
				return t;
			});
		} else {
			tn = null;
			g = null;
			logex = null;
		}
	}
	private final org.slf4j.Logger logger;

	private void submit(Runnable run) {
		if (async) try {
			logex.submit(run);
		} catch (RejectedExecutionException e) {
			run.run();
		}
		else run.run();
	}

	private Logger(org.slf4j.Logger logger) {
		super();
		this.logger = logger;
	}

	public static final Logger getLogger(CharSequence name) {
		return new Logger(org.slf4j.LoggerFactory.getLogger(name.toString()));
	}

	public static final Logger getLogger(Class<?> clazz) {
		return new Logger(org.slf4j.LoggerFactory.getLogger(clazz));
	}

	public Logger(CharSequence name) {
		this(org.slf4j.LoggerFactory.getLogger(name.toString()));
	}

	public Logger(Class<?> clazz) {
		this(org.slf4j.LoggerFactory.getLogger(clazz));
	}

	public CharSequence getName() {
		return logger.getName();
	}

	public boolean isTraceEnabled() {
		return logger.isTraceEnabled();
	}

	public boolean trace(CharSequence msg) {
		logger.trace(msg.toString());
		return true;
	}

	public boolean trace(Supplier<CharSequence> msg) {
		if (logger.isTraceEnabled()) {
			CharSequence m = msg.get();
			if (null != m) logger.trace(m.toString());
		}
		return true;
	}

	public boolean isDebugEnabled() {
		return logger.isDebugEnabled();
	}

	public boolean debug(CharSequence msg) {
		submit(() -> logger.debug(msg.toString()));
		return true;
	}

	public boolean debug(Supplier<CharSequence> msg) {
		if (logger.isDebugEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.debug(m.toString()));
		}
		return true;
	}

	public boolean isInfoEnabled() {
		return logger.isInfoEnabled();
	}

	public boolean info(CharSequence msg) {
		submit(() -> logger.info(msg.toString()));
		return true;
	}

	public boolean info(Supplier<CharSequence> msg) {
		if (logger.isInfoEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.info(m.toString()));
		}
		return true;
	}

	public boolean isWarnEnabled() {
		return logger.isWarnEnabled();
	}

	public boolean warn(CharSequence msg) {
		submit(() -> logger.warn(msg.toString()));
		return true;
	}

	public boolean warn(Supplier<CharSequence> msg) {
		if (logger.isWarnEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.warn(m.toString()));
		}
		return true;
	}

	public boolean isErrorEnabled() {
		return logger.isErrorEnabled();
	}

	public boolean error(CharSequence msg) {
		submit(() -> logger.error(msg.toString()));
		return true;
	}

	public boolean error(Supplier<CharSequence> msg) {
		if (logger.isErrorEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.error(m.toString()));
		}
		return true;
	}

	public boolean trace(Supplier<CharSequence> msg, Throwable t) {
		if (logger.isTraceEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.trace(m.toString(), t));
		}
		return true;
	}

	public boolean debug(Supplier<CharSequence> msg, Throwable t) {
		if (logger.isDebugEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.debug(m.toString(), t));
		}
		return true;
	}

	public boolean info(Supplier<CharSequence> msg, Throwable t) {
		if (logger.isInfoEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.info(m.toString(), t));
		}
		return true;
	}

	public boolean warn(Supplier<CharSequence> msg, Throwable t) {
		if (logger.isWarnEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.warn(m.toString(), t));
		}
		return true;
	}

	public boolean error(Supplier<CharSequence> msg, Throwable t) {
		if (logger.isErrorEnabled()) {
			CharSequence m = msg.get();
			if (null != m) submit(() -> logger.error(m.toString(), t));
		}
		return true;
	}

	public boolean trace(CharSequence msg, Throwable t) {
		submit(() -> logger.trace(msg.toString(), t));
		return true;
	}

	public boolean debug(CharSequence msg, Throwable t) {
		submit(() -> logger.debug(msg.toString(), t));
		return true;
	}

	public boolean info(CharSequence msg, Throwable t) {
		submit(() -> logger.info(msg.toString(), t));
		return true;
	}

	public boolean warn(CharSequence msg, Throwable t) {
		submit(() -> logger.warn(msg.toString(), t));
		return true;
	}

	public boolean error(CharSequence msg, Throwable t) {
		submit(() -> logger.error(msg.toString(), t));
		return true;
	}

	/** Old style */
	public boolean trace(CharSequence format, Object arg) {
		submit(() -> logger.trace(format.toString(), arg));
		return true;
	}

	public boolean trace(CharSequence format, Object arg1, Object arg2) {
		submit(() -> logger.trace(format.toString(), arg1, arg2));
		return true;
	}

	public boolean trace(CharSequence format, Object... arguments) {
		submit(() -> logger.trace(format.toString(), arguments));
		return true;
	}

	public boolean debug(CharSequence format, Object arg) {
		submit(() -> logger.debug(format.toString(), arg));
		return true;
	}

	public boolean debug(CharSequence format, Object arg1, Object arg2) {
		submit(() -> logger.debug(format.toString(), arg1, arg2));
		return true;
	}

	public boolean debug(CharSequence format, Object... arguments) {
		submit(() -> logger.debug(format.toString(), arguments));
		return true;
	}

	public boolean info(CharSequence format, Object arg) {
		submit(() -> logger.info(format.toString(), arg));
		return true;
	}

	public boolean info(CharSequence format, Object arg1, Object arg2) {
		submit(() -> logger.info(format.toString(), arg1, arg2));
		return true;
	}

	public boolean info(CharSequence format, Object... arguments) {
		submit(() -> logger.info(format.toString(), arguments));
		return true;
	}

	public boolean warn(CharSequence format, Object arg) {
		submit(() -> logger.warn(format.toString(), arg));
		return true;
	}

	public boolean warn(CharSequence format, Object... arguments) {
		submit(() -> logger.warn(format.toString(), arguments));
		return true;
	}

	public boolean warn(CharSequence format, Object arg1, Object arg2) {
		submit(() -> logger.warn(format.toString(), arg1, arg2));
		return true;
	}

	public boolean error(CharSequence format, Object arg) {
		submit(() -> logger.error(format.toString(), arg));
		return true;
	}

	public boolean error(CharSequence format, Object arg1, Object arg2) {
		submit(() -> logger.error(format.toString(), arg1, arg2));
		return true;
	}

	public boolean error(CharSequence format, Object... arguments) {
		submit(() -> logger.error(format.toString(), arguments));
		return true;
	}

	/** Ignore, we will never use Marker */
	// public boolean isTraceEnabled(Marker marker)
	// public void trace(Marker marker, CharSequence msg)
	// public void trace(Marker marker, CharSequence format, Object arg)
	// public void trace(Marker marker, CharSequence format, Object arg1, Object
	// arg2)
	// public void trace(Marker marker, CharSequence format, Object... argArray)
	// public void trace(Marker marker, CharSequence msg, Throwable t)
	// public boolean isDebugEnabled(Marker marker)
	// public void debug(Marker marker, CharSequence msg)
	// public void debug(Marker marker, CharSequence format, Object arg)
	// public void debug(Marker marker, CharSequence format, Object arg1, Object
	// arg2)
	// public void debug(Marker marker, CharSequence format, Object...
	// arguments)
	// public void debug(Marker marker, CharSequence msg, Throwable t)
	// public boolean isInfoEnabled(Marker marker)
	// public void info(Marker marker, CharSequence msg)
	// public void info(Marker marker, CharSequence format, Object arg)
	// public void info(Marker marker, CharSequence format, Object arg1, Object
	// arg2)
	// public void info(Marker marker, CharSequence format, Object... arguments)
	// public void info(Marker marker, CharSequence msg, Throwable t)
	// public boolean isWarnEnabled(Marker marker)
	// public void warn(Marker marker, CharSequence msg)
	// public void warn(Marker marker, CharSequence format, Object arg)
	// public void warn(Marker marker, CharSequence format, Object arg1, Object
	// arg2)
	// public void warn(Marker marker, CharSequence format, Object... arguments)
	// public void warn(Marker marker, CharSequence msg, Throwable t)
	// public boolean isErrorEnabled(Marker marker)
	// public void error(Marker marker, CharSequence msg)
	// public void error(Marker marker, CharSequence format, Object arg)
	// public void error(Marker marker, CharSequence format, Object arg1, Object
	// arg2)
	// public void error(Marker marker, CharSequence format, Object...
	// arguments)
	// public void error(Marker marker, CharSequence msg, Throwable t)
}