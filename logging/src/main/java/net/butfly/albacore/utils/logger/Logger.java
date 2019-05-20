package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.utils.logger.LogExec.tryExec;
import static net.butfly.albacore.utils.logger.Loggers.ing;
import static net.butfly.albacore.utils.logger.Loggers.shrinkClassname;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.slf4j.impl.Log4jLoggerAdapter;

import net.butfly.albacore.utils.logger.Loggers.Logging;

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
	private static final long serialVersionUID = -1940330974751419775L;
	static final Map<String, Logger> loggers = new ConcurrentHashMap<>();
	private static Logger normalLogger = Logger.getLogger(Log4jHelper.class);
	static {
		Log4jHelper.checkConf();
		Log4jHelper.fixMDC();
	}

	public final org.slf4j.Logger logger;

	private Logger(org.slf4j.Logger logger) {
		super();
		this.logger = logger;
	}


	// factory
	public static final Logger getLogger(CharSequence name) {
		String key = name.toString();
		Logging ing = null;
		try {
			ing = ing(Class.forName(key));
		} catch (Exception e) { // not class name
		}
		if (null == ing) {
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			ing = ing(cl, Package.getPackage(key));
		}
		return get(key, ing);
	}

	public static final Logger getLogger(Class<?> clazz) {
		String key = clazz.getName();
		Logging ing;
		ing = ing(clazz);
		if (null == ing) ing = ing(clazz.getClassLoader(), clazz.getPackage());
		return get(key, ing);
	}

	private static final Logger get(String key, Logging ing) {
		return loggers.computeIfAbsent(key, k -> {
			org.slf4j.Logger slf = LoggerFactory.getLogger(key);
			if (null != ing && ing.force()) {
				if (slf instanceof Log4jLoggerAdapter) slf = Log4jHelper.changeLevel((Log4jLoggerAdapter) slf, ing.value());
				else slf.warn("Logging defined on key: [" + ing.toString() + "], " //
						+ "but not valid for only log4j impl is supported. Impl detected: " + slf.getClass());
			}
			return new Logger(slf);
		});

	}

	public Logger(CharSequence name) {
		this(LoggerFactory.getLogger(name.toString()));
	}

	// basic
	public Logger(Class<?> clazz) {
		this(LoggerFactory.getLogger(clazz.getName()));
	}

	public CharSequence getName() { return logger.getName(); }

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
	public boolean isTraceEnabled() { return logger.isTraceEnabled(); }

	public boolean trace(CharSequence msg) {
		if (null == msg) return false;
		return tryExec(() -> logger.trace(msg.toString()));
	}

	public boolean trace(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.trace(s.toString());
		});
	}

	public boolean isDebugEnabled() { return logger.isDebugEnabled(); }

	public boolean debug(CharSequence msg) {
		if (null == msg) return false;
		return tryExec(() -> logger.debug(msg.toString()));
	}

	public boolean debug(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.debug(s.toString());
		});
	}

	public boolean isInfoEnabled() { return logger.isInfoEnabled(); }

	public boolean info(CharSequence msg) {
		if (null == msg) return false;
		return tryExec(() -> logger.info(msg.toString()));
	}

	public boolean info(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.info(s.toString());
		});
	}

	public boolean isWarnEnabled() { return logger.isWarnEnabled(); }

	public boolean warn(CharSequence msg) {
		if (null == msg) return false;
		return tryExec(() -> logger.warn(msg.toString()));
	}

	public boolean warn(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.warn(s.toString());
		});
	}

	public boolean isErrorEnabled() { return logger.isErrorEnabled(); }

	public boolean error(CharSequence msg) {
		if (null == msg) return false;
		return tryExec(() -> logger.error(msg.toString()));
	}

	public boolean error(Supplier<CharSequence> msg) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.error(s.toString());
		});
	}

	public static boolean logs(Level level, CharSequence msg) {
		String c = Thread.currentThread().getStackTrace()[2].getClassName().split("$")[0];
		return getLogger(shrinkClassname(c)).log(level, () -> msg);
	}

	public static boolean logs(Level level, CharSequence msg, Throwable t) {
		String c = Thread.currentThread().getStackTrace()[2].getClassName().split("$")[0];
		return getLogger(shrinkClassname(c)).log(level, () -> msg, t);
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
		return tryExec(() -> logger.trace(msg.toString(), t));
	}

	public boolean debug(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> logger.debug(msg.toString(), t));
	}

	public boolean info(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> logger.info(msg.toString(), t));
	}

	public boolean warn(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> logger.warn(msg.toString(), t));
	}

	public boolean error(CharSequence msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> logger.error(msg.toString(), t));
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
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.trace(s.toString(), t);
		});
	}

	public boolean debug(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.debug(s.toString(), t);
		});
	}

	public boolean info(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.info(s.toString(), t);
		});
	}

	public boolean warn(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.warn(s.toString(), t);
		});
	}

	public boolean error(Supplier<CharSequence> msg, Throwable t) {
		if (null == msg) return false;
		return tryExec(() -> {
			CharSequence s = msg.get();
			if (null != s) logger.error(s.toString(), t);
		});
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
	static Map<org.slf4j.event.Level, org.apache.log4j.Level> LEVELS_SLF_TO_LOG4J = new ConcurrentHashMap<>();
	static {
		LEVELS_SLF_TO_LOG4J.put(org.slf4j.event.Level.ERROR, org.apache.log4j.Level.ERROR);
		LEVELS_SLF_TO_LOG4J.put(org.slf4j.event.Level.WARN, org.apache.log4j.Level.WARN);
		LEVELS_SLF_TO_LOG4J.put(org.slf4j.event.Level.INFO, org.apache.log4j.Level.INFO);
		LEVELS_SLF_TO_LOG4J.put(org.slf4j.event.Level.DEBUG, org.apache.log4j.Level.DEBUG);
		LEVELS_SLF_TO_LOG4J.put(org.slf4j.event.Level.TRACE, org.apache.log4j.Level.TRACE);
	}

	public final Log4js impl4j = new Log4js();

	public class Log4js {
		public org.apache.log4j.Logger logger() {
			return Log4jHelper.log4j((Log4jLoggerAdapter) logger);
		}

		// find appender for log (with setting prefix)
		@SuppressWarnings("unchecked")
		public List<Appender> appenders() {
			org.apache.log4j.Logger l = logger();
			Enumeration<Appender> apds;
			// find appender for log (with setting prefix)
			List<Appender> r = new ArrayList<>();
			while (!(apds = l.getAllAppenders()).hasMoreElements()) if (null == (l = (org.apache.log4j.Logger) l.getParent())) //
				return r;
			while (apds.hasMoreElements()) r.add(apds.nextElement());
			return r;
		}

		public Appender appender() {
			List<Appender> apds = appenders();
			if (apds.isEmpty()) return null;
			if (apds.size() > 1) normalLogger.warn("More appenders defined on JsonLogger, only first is force to JsonLayout");
			return apds.get(0);
		}

		public void suffix(String suffix) {
			Appender a = appender();
			if (a instanceof FileAppender) {
				FileAppender fa = (FileAppender) a;
				String[] segs = fa.getFile().split("\\.", 2);
				String fn = segs.length == 2 ? segs[0] + suffix + "." + segs[1] : fa.getFile() + suffix;
				normalLogger.info("Logger [" + logger.getName() + "] has been binded on FileAppender [" + fa.getName()
						+ "], filename of the appender has been adjusted from [" + fa.getFile() + "] into [" + fn + "].");
				fa.setFile(fn);
			}
		}
	}
}