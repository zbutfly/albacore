package net.butfly.albacore.utils.logger;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * </p>
 * 
 * <pre>
 * FINEST  -&gt; TRACE
 * FINER   -&gt; DEBUG
 * FINE    -&gt; DEBUG
 * INFO    -&gt; INFO
 * WARNING -&gt; WARN
 * SEVERE  -&gt; ERROR
 * </pre>
 * <p>
 * <b>Programmatic installation:</b>
 * </p>
 * 
 * <pre>
 * 
 * @author butfly
 */
public class Logger implements Serializable {
	public static final String PROP_LOGGER_ASYNC = "albacore.logger.async.enable";// true
	public static final String PROP_LOGGER_PARALLELISM = "albacore.logger.parallelism";// true
	public static final String PROP_LOGGER_QUEUE_SIZE = "albacore.logger.queue.size";// true
	private static final long serialVersionUID = -1940330974751419775L;

	public static final ExecutorService logex;
	static {
		if (Boolean.parseBoolean(System.getProperty(PROP_LOGGER_ASYNC, "true"))) {
			int parallelism = Integer.parseInt(System.getProperty(PROP_LOGGER_PARALLELISM, "8"));
			int queueSize = Integer.parseInt(System.getProperty(PROP_LOGGER_QUEUE_SIZE, "1024"));
			AtomicInteger tn = new AtomicInteger();
			ThreadGroup g = new ThreadGroup("AlbacoreLoggerThread");
			logex = new ThreadPoolExecutor(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS, //
					new LinkedBlockingQueue<Runnable>(queueSize), r -> {
						Thread t = new Thread(g, r, "AlbacoreLoggerThread#" + tn.getAndIncrement());
						t.setDaemon(true);
						return t;
					}, (r, ex) -> {
						// process rejected...ignore
					});
		} else {
			logex = null;
		}
	}

	public static boolean logexec(Runnable r) {
		try {
			logex.execute(r);
			return true;
		} catch (RejectedExecutionException e) {
			return false;
		}
	}

	private final org.slf4j.Logger logger;

	private Logger(org.slf4j.Logger logger) {
		super();
		this.logger = logger;
	}

	static final Map<String, Logger> loggers = new ConcurrentHashMap<>();

	// factory
	public static final Logger getLogger(CharSequence name) {
		return loggers.computeIfAbsent(name.toString(), n -> new Logger(LoggerFactory.getLogger(name.toString())));
	}

	public static final Logger getLogger(Class<?> clazz) {
		return loggers.computeIfAbsent(clazz.getName(), c -> new Logger(LoggerFactory.getLogger(clazz.getName())));
	}

	public Logger(CharSequence name) {
		this(LoggerFactory.getLogger(name.toString()));
	}

	// basic
	public Logger(Class<?> clazz) {
		this(LoggerFactory.getLogger(clazz.getName()));
	}

	public CharSequence getName() {
		return logger.getName();
	}

	public boolean isLoggable(Level level) {
		switch (level) {
		case TRACE:
			return logger.isTraceEnabled();
		case DEBUG:
			return logger.isDebugEnabled();
		case INFO:
			return logger.isInfoEnabled();
		case WARN:
			return logger.isWarnEnabled();
		case ERROR:
			return logger.isErrorEnabled();
		default:
			return false;
		}
	}

	public boolean log(Level level, CharSequence msg) {
		return log(level, () -> msg);
	}

	public boolean log(Level level, Supplier<CharSequence> msg) {
		switch (level) {
		case TRACE:
			return trace(msg);
		case DEBUG:
			return debug(msg);
		case INFO:
			return info(msg);
		case WARN:
			return warn(msg);
		case ERROR:
			return error(msg);
		default:
			return false;
		}
	}

	// slf4j style
	public boolean isTraceEnabled() {
		return logger.isTraceEnabled();
	}

	public boolean trace(CharSequence msg) {
		if (null == msg) return false;
		logex.execute(() -> logger.trace(msg.toString()));
		return true;
	}

	public boolean trace(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.trace(s.toString());
		});
		return true;
	}

	public boolean isDebugEnabled() {
		return logger.isDebugEnabled();
	}

	public boolean debug(CharSequence msg) {
		if (null == msg) return false;
		logex.execute(() -> logger.debug(msg.toString()));
		return true;
	}

	public boolean debug(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.debug(s.toString());
		});
		return true;
	}

	public boolean isInfoEnabled() {
		return logger.isInfoEnabled();
	}

	public boolean info(CharSequence msg) {
		if (null == msg) return false;
		logex.execute(() -> logger.info(msg.toString()));
		return true;
	}

	public boolean info(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.info(s.toString());
		});
		return true;
	}

	public boolean isWarnEnabled() {
		return logger.isWarnEnabled();
	}

	public boolean warn(CharSequence msg) {
		if (null == msg) return false;
		logex.execute(() -> logger.warn(msg.toString()));
		return true;
	}

	public boolean warn(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.warn(s.toString());
		});
		return true;
	}

	public boolean isErrorEnabled() {
		return logger.isErrorEnabled();
	}

	public boolean error(CharSequence msg) {
		if (null == msg) return false;
		logex.execute(() -> logger.error(msg.toString()));
		return true;
	}

	public boolean error(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.error(s.toString());
		});
		return true;
	}

	// extends args
	public boolean log(Level level, CharSequence msg, Throwable t) {
		switch (level) {
		case TRACE:
			return trace(msg, t);
		case DEBUG:
			return debug(msg, t);
		case INFO:
			return info(msg, t);
		case WARN:
			return warn(msg, t);
		case ERROR:
			return error(msg, t);
		default:
			return false;
		}
	}

	public boolean trace(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> logger.trace(msg.toString(), t));
		return true;
	}

	public boolean debug(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> logger.debug(msg.toString(), t));
		return true;
	}

	public boolean info(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> logger.info(msg.toString(), t));
		return true;
	}

	public boolean warn(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> logger.warn(msg.toString(), t));
		return true;
	}

	public boolean error(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> logger.error(msg.toString(), t));
		return true;
	}

	public boolean log(Level level, Supplier<CharSequence> msg, Throwable t) {
		switch (level) {
		case TRACE:
			return trace(msg, t);
		case DEBUG:
			return debug(msg, t);
		case INFO:
			return info(msg, t);
		case WARN:
			return warn(msg, t);
		case ERROR:
			return error(msg, t);
		default:
			return false;
		}
	}

	public boolean trace(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.trace(s.toString(), t);
		});
		return true;
	}

	public boolean debug(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.debug(s.toString(), t);
		});
		return true;
	}

	public boolean info(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.info(s.toString(), t);
		});
		return true;
	}

	public boolean warn(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.warn(s.toString(), t);
		});
		return true;
	}

	public boolean error(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		logex.execute(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.error(s.toString(), t);
		});
		return true;
	}

	// /** Old style */
	// public boolean trace(CharSequence format, Object arg) {
	// submit.accept(() -> trace(format.toString(), arg));
	// return true;
	// }
	//
	// public boolean trace(CharSequence format, Object arg1, Object arg2) {
	// submit.accept(() -> trace(format.toString(), arg1, arg2));
	// return true;
	// }
	//
	// public boolean trace(CharSequence format, Object... arguments) {
	// submit.accept(() -> trace(format.toString(), arguments));
	// return true;
	// }
	//
	// public boolean debug(CharSequence format, Object arg) {
	// submit.accept(() -> debug(format.toString(), arg));
	// return true;
	// }
	//
	// public boolean debug(CharSequence format, Object arg1, Object arg2) {
	// submit.accept(() -> debug(format.toString(), arg1, arg2));
	// return true;
	// }
	//
	// public boolean debug(CharSequence format, Object... arguments) {
	// submit.accept(() -> debug(format.toString(), arguments));
	// return true;
	// }
	//
	// public boolean info(CharSequence format, Object arg) {
	// submit.accept(() -> info(format.toString(), arg));
	// return true;
	// }
	//
	// public boolean info(CharSequence format, Object arg1, Object arg2) {
	// submit.accept(() -> info(format.toString(), arg1, arg2));
	// return true;
	// }
	//
	// public boolean info(CharSequence format, Object... arguments) {
	// submit.accept(() -> info(format.toString(), arguments));
	// return true;
	// }
	//
	// public boolean warn(CharSequence format, Object arg) {
	// submit.accept(() -> warn(format.toString(), arg));
	// return true;
	// }
	//
	// public boolean warn(CharSequence format, Object... arguments) {
	// submit.accept(() -> warn(format.toString(), arguments));
	// return true;
	// }
	//
	// public boolean warn(CharSequence format, Object arg1, Object arg2) {
	// submit.accept(() -> warn(format.toString(), arg1, arg2));
	// return true;
	// }
	//
	// public boolean error(CharSequence format, Object arg) {
	// submit.accept(() -> error(format.toString(), arg));
	// return true;
	// }
	//
	// public boolean error(CharSequence format, Object arg1, Object arg2) {
	// submit.accept(() -> error(format.toString(), arg1, arg2));
	// return true;
	// }
	//
	// public boolean error(CharSequence format, Object... arguments) {
	// submit.accept(() -> error(format.toString(), arguments));
	// return true;
	// }

	/** Ignore, we will never use Marker */
	// public boolean isTraceEnabled(Marker marker)
	// public void trace(Marker marker, CharSequence msg)
	// public void trace(Marker marker, CharSequence format, Object arg)
	// public void trace(Marker marker, CharSequence format, Object arg1, Object arg2)
	// public void trace(Marker marker, CharSequence format, Object... argArray)
	// public void trace(Marker marker, CharSequence msg, Throwable t)
	// public boolean isDebugEnabled(Marker marker)
	// public void debug(Marker marker, CharSequence msg)
	// public void debug(Marker marker, CharSequence format, Object arg)
	// public void debug(Marker marker, CharSequence format, Object arg1, Object arg2)
	// public void debug(Marker marker, CharSequence format, Object... arguments)
	// public void debug(Marker marker, CharSequence msg, Throwable t)
	// public boolean isInfoEnabled(Marker marker)
	// public void info(Marker marker, CharSequence msg)
	// public void info(Marker marker, CharSequence format, Object arg)
	// public void info(Marker marker, CharSequence format, Object arg1, Object arg2)
	// public void info(Marker marker, CharSequence format, Object... arguments)
	// public void info(Marker marker, CharSequence msg, Throwable t)
	// public boolean isWarnEnabled(Marker marker)
	// public void warn(Marker marker, CharSequence msg)
	// public void warn(Marker marker, CharSequence format, Object arg)
	// public void warn(Marker marker, CharSequence format, Object arg1, Object arg2)
	// public void warn(Marker marker, CharSequence format, Object... arguments)
	// public void warn(Marker marker, CharSequence msg, Throwable t)
	// public boolean isErrorEnabled(Marker marker)
	// public void error(Marker marker, CharSequence msg)
	// public void error(Marker marker, CharSequence format, Object arg)
	// public void error(Marker marker, CharSequence format, Object arg1, Object arg2)
	// public void error(Marker marker, CharSequence format, Object... arguments)
	// public void error(Marker marker, CharSequence msg, Throwable t)
}