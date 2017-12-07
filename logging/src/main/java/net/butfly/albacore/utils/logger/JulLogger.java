//package net.butfly.albacore.utils.logger;
//
//import java.io.Serializable;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.function.Consumer;
//import java.util.function.Supplier;
//import java.util.logging.Level;
//
///**
// * </p>
// * 
// * <pre>
// * FINEST  -&gt; TRACE
// * FINER   -&gt; DEBUG
// * FINE    -&gt; DEBUG
// * INFO    -&gt; INFO
// * WARNING -&gt; WARN
// * SEVERE  -&gt; ERROR
// * </pre>
// * <p>
// * <b>Programmatic installation:</b>
// * </p>
// * 
// * <pre>
// * 
// * @author butfly
// */
//public class JulLogger implements Serializable {
//	private static final long serialVersionUID = 3734706091058108037L;
//	public static final String PROP_LOGGER_ASYNC = "albacore.logger.async.enable";// true
//	public static final String PROP_LOGGER_PARALLELISM = "albacore.logger.parallelism";// true
//	public static final String PROP_LOGGER_QUEUE_SIZE = "albacore.logger.queue.size";// true
//	private static Map<org.slf4j.event.Level, Level> slfToJul = new ConcurrentHashMap<>();
//	private static Map<Level, org.slf4j.event.Level> julToSlf = new ConcurrentHashMap<>();
//
//	private static final Consumer<Runnable> submit;
//	public static final ExecutorService logex;
//	static {
//		/* define jul to slf4j */
//		java.util.logging.LogManager.getLogManager().reset();
//		org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger();
//		org.slf4j.bridge.SLF4JBridgeHandler.install();
//		java.util.logging.Logger.getLogger("global").setLevel(Level.FINEST);
//
//		slfToJul.put(org.slf4j.event.Level.ERROR, Level.SEVERE);
//		slfToJul.put(org.slf4j.event.Level.WARN, Level.WARNING);
//		slfToJul.put(org.slf4j.event.Level.INFO, Level.INFO);
//		slfToJul.put(org.slf4j.event.Level.DEBUG, Level.FINE);
//		slfToJul.put(org.slf4j.event.Level.TRACE, Level.FINEST);
//
//		julToSlf.put(Level.OFF, org.slf4j.event.Level.ERROR);
//		julToSlf.put(Level.SEVERE, org.slf4j.event.Level.ERROR);
//		julToSlf.put(Level.WARNING, org.slf4j.event.Level.WARN);
//		julToSlf.put(Level.INFO, org.slf4j.event.Level.INFO);
//		julToSlf.put(Level.CONFIG, org.slf4j.event.Level.INFO);
//		julToSlf.put(Level.FINE, org.slf4j.event.Level.DEBUG);
//		julToSlf.put(Level.FINER, org.slf4j.event.Level.DEBUG);
//		julToSlf.put(Level.FINEST, org.slf4j.event.Level.TRACE);
//		julToSlf.put(Level.ALL, org.slf4j.event.Level.TRACE);
//
//		if (Boolean.parseBoolean(System.getProperty(PROP_LOGGER_ASYNC, "true"))) {
//			int parallelism = Integer.parseInt(System.getProperty(PROP_LOGGER_PARALLELISM, "8"));
//			int queueSize = Integer.parseInt(System.getProperty(PROP_LOGGER_QUEUE_SIZE, "1024"));
//			AtomicInteger tn = new AtomicInteger();
//			ThreadGroup g = new ThreadGroup("AlbacoreLoggerThread");
//			logex = new ThreadPoolExecutor(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS, //
//					new LinkedBlockingQueue<Runnable>(queueSize), r -> {
//						Thread t = new Thread(g, r, "AlbacoreLoggerThread#" + tn.getAndIncrement());
//						t.setDaemon(true);
//						return t;
//					}, (r, ex) -> {
//						// process rejected...ignore
//					});
//			submit = logex::submit;
//		} else {
//			logex = null;
//			submit = Runnable::run;
//		}
//	}
//	// private final org.slf4j.Logger logger;
//	private java.util.logging.Logger logger;
//
//	private JulLogger(java.util.logging.Logger logger) {
//		super();
//		this.logger = logger;
//	}
//
//	static final Map<String, JulLogger> loggers = new ConcurrentHashMap<>();
//
//	// factory
//	public static final JulLogger getLogger(CharSequence name) {
//		return loggers.computeIfAbsent(name.toString(), n -> new JulLogger(java.util.logging.Logger.getLogger(name.toString())));
//	}
//
//	public static final JulLogger getLogger(Class<?> clazz) {
//		return loggers.computeIfAbsent(clazz.getName(), c -> new JulLogger(java.util.logging.Logger.getLogger(clazz.getName())));
//	}
//
//	public JulLogger(CharSequence name) {
//		this(java.util.logging.Logger.getLogger(name.toString()));
//	}
//
//	// basic
//	public JulLogger(Class<?> clazz) {
//		this(java.util.logging.Logger.getLogger(clazz.getName()));
//	}
//
//	public CharSequence getName() {
//		return logger.getName();
//	}
//
//	public boolean isLoggable(Level level) {
//		Level l = null;
//		java.util.logging.Logger log = logger;
//		while (null != log && null == (l = logger.getLevel()))
//			log = log.getParent();
//		if (null == l) return false;
//		else return l.intValue() <= level.intValue();
//	}
//
//	public boolean isLoggable(org.slf4j.event.Level level) {
//		return isLoggable(slfToJul.get(level));
//	}
//
//	public boolean log(org.slf4j.event.Level l, CharSequence msg) {
//		return log(slfToJul.get(l), msg);
//	}
//
//	public boolean log(Level level, CharSequence msg) {
//		logger.log(level, () -> null == msg ? null : msg.toString());
//		return true;
//	}
//
//	public boolean log(org.slf4j.event.Level l, Supplier<CharSequence> msg) {
//		return log(slfToJul.get(l), msg);
//	}
//
//	public boolean log(Level level, Supplier<CharSequence> msg) {
//		if (isLoggable(level)) submit.accept(() -> {
//			CharSequence m = msg.get();
//			if (null != m) logger.log(level, () -> m.toString());
//		});
//		return true;
//	}
//
//	// slf4j style
//	public boolean isTraceEnabled() {
//		return isLoggable(Level.FINEST);
//	}
//
//	public boolean trace(CharSequence msg) {
//		return log(Level.FINEST, () -> msg);
//	}
//
//	public boolean trace(Supplier<CharSequence> msg) {
//		return log(Level.FINEST, msg);
//	}
//
//	public boolean isDebugEnabled() {
//		return isLoggable(Level.FINER);
//	}
//
//	public boolean debug(CharSequence msg) {
//		return log(Level.FINE, () -> msg);
//	}
//
//	public boolean debug(Supplier<CharSequence> msg) {
//		return log(Level.FINE, msg);
//	}
//
//	public boolean isInfoEnabled() {
//		return isLoggable(Level.INFO);
//	}
//
//	public boolean info(CharSequence msg) {
//		return log(Level.INFO, () -> msg);
//	}
//
//	public boolean info(Supplier<CharSequence> msg) {
//		return log(Level.INFO, msg);
//	}
//
//	public boolean isWarnEnabled() {
//		return isLoggable(Level.WARNING);
//	}
//
//	public boolean warn(CharSequence msg) {
//		return log(Level.WARNING, () -> msg);
//	}
//
//	public boolean warn(Supplier<CharSequence> msg) {
//		return log(Level.WARNING, msg);
//	}
//
//	public boolean isErrorEnabled() {
//		return isLoggable(Level.SEVERE);
//	}
//
//	public boolean error(CharSequence msg) {
//		return log(Level.SEVERE, () -> msg);
//	}
//
//	public boolean error(Supplier<CharSequence> msg) {
//		return log(Level.SEVERE, msg);
//	}
//
//	// extends args
//	public boolean log(Level level, CharSequence msg, Throwable t) {
//		if (logger.isLoggable(level)) submit.accept(() -> {
//			if (null != msg) logger.log(level, msg.toString(), t);
//		});
//		return true;
//	}
//
//	public boolean trace(CharSequence msg, Throwable t) {
//		return log(Level.FINEST, msg, t);
//	}
//
//	public boolean debug(CharSequence msg, Throwable t) {
//		return log(Level.FINE, msg, t);
//	}
//
//	public boolean info(CharSequence msg, Throwable t) {
//		return log(Level.INFO, msg, t);
//	}
//
//	public boolean warn(CharSequence msg, Throwable t) {
//		return log(Level.WARNING, msg, t);
//	}
//
//	public boolean error(CharSequence msg, Throwable t) {
//		return log(Level.SEVERE, msg, t);
//	}
//
//	public boolean log(Level level, Supplier<CharSequence> msg, Throwable t) {
//		if (logger.isLoggable(level)) submit.accept(() -> {
//			String m = msg.toString();
//			if (null != m) logger.log(level, m.toString(), t);
//		});
//		return true;
//	}
//
//	public boolean trace(Supplier<CharSequence> msg, Throwable t) {
//		return log(Level.FINEST, msg, t);
//	}
//
//	public boolean debug(Supplier<CharSequence> msg, Throwable t) {
//		return log(Level.FINE, msg, t);
//	}
//
//	public boolean info(Supplier<CharSequence> msg, Throwable t) {
//		return log(Level.INFO, msg, t);
//	}
//
//	public boolean warn(Supplier<CharSequence> msg, Throwable t) {
//		return log(Level.WARNING, msg, t);
//	}
//
//	public boolean error(Supplier<CharSequence> msg, Throwable t) {
//		return log(Level.SEVERE, msg, t);
//	}
//
//	// /** Old style */
//	// public boolean trace(CharSequence format, Object arg) {
//	// submit.accept(() -> trace(format.toString(), arg));
//	// return true;
//	// }
//	//
//	// public boolean trace(CharSequence format, Object arg1, Object arg2) {
//	// submit.accept(() -> trace(format.toString(), arg1, arg2));
//	// return true;
//	// }
//	//
//	// public boolean trace(CharSequence format, Object... arguments) {
//	// submit.accept(() -> trace(format.toString(), arguments));
//	// return true;
//	// }
//	//
//	// public boolean debug(CharSequence format, Object arg) {
//	// submit.accept(() -> debug(format.toString(), arg));
//	// return true;
//	// }
//	//
//	// public boolean debug(CharSequence format, Object arg1, Object arg2) {
//	// submit.accept(() -> debug(format.toString(), arg1, arg2));
//	// return true;
//	// }
//	//
//	// public boolean debug(CharSequence format, Object... arguments) {
//	// submit.accept(() -> debug(format.toString(), arguments));
//	// return true;
//	// }
//	//
//	// public boolean info(CharSequence format, Object arg) {
//	// submit.accept(() -> info(format.toString(), arg));
//	// return true;
//	// }
//	//
//	// public boolean info(CharSequence format, Object arg1, Object arg2) {
//	// submit.accept(() -> info(format.toString(), arg1, arg2));
//	// return true;
//	// }
//	//
//	// public boolean info(CharSequence format, Object... arguments) {
//	// submit.accept(() -> info(format.toString(), arguments));
//	// return true;
//	// }
//	//
//	// public boolean warn(CharSequence format, Object arg) {
//	// submit.accept(() -> warn(format.toString(), arg));
//	// return true;
//	// }
//	//
//	// public boolean warn(CharSequence format, Object... arguments) {
//	// submit.accept(() -> warn(format.toString(), arguments));
//	// return true;
//	// }
//	//
//	// public boolean warn(CharSequence format, Object arg1, Object arg2) {
//	// submit.accept(() -> warn(format.toString(), arg1, arg2));
//	// return true;
//	// }
//	//
//	// public boolean error(CharSequence format, Object arg) {
//	// submit.accept(() -> error(format.toString(), arg));
//	// return true;
//	// }
//	//
//	// public boolean error(CharSequence format, Object arg1, Object arg2) {
//	// submit.accept(() -> error(format.toString(), arg1, arg2));
//	// return true;
//	// }
//	//
//	// public boolean error(CharSequence format, Object... arguments) {
//	// submit.accept(() -> error(format.toString(), arguments));
//	// return true;
//	// }
//
//	/** Ignore, we will never use Marker */
//	// public boolean isTraceEnabled(Marker marker)
//	// public void trace(Marker marker, CharSequence msg)
//	// public void trace(Marker marker, CharSequence format, Object arg)
//	// public void trace(Marker marker, CharSequence format, Object arg1, Object arg2)
//	// public void trace(Marker marker, CharSequence format, Object... argArray)
//	// public void trace(Marker marker, CharSequence msg, Throwable t)
//	// public boolean isDebugEnabled(Marker marker)
//	// public void debug(Marker marker, CharSequence msg)
//	// public void debug(Marker marker, CharSequence format, Object arg)
//	// public void debug(Marker marker, CharSequence format, Object arg1, Object arg2)
//	// public void debug(Marker marker, CharSequence format, Object... arguments)
//	// public void debug(Marker marker, CharSequence msg, Throwable t)
//	// public boolean isInfoEnabled(Marker marker)
//	// public void info(Marker marker, CharSequence msg)
//	// public void info(Marker marker, CharSequence format, Object arg)
//	// public void info(Marker marker, CharSequence format, Object arg1, Object arg2)
//	// public void info(Marker marker, CharSequence format, Object... arguments)
//	// public void info(Marker marker, CharSequence msg, Throwable t)
//	// public boolean isWarnEnabled(Marker marker)
//	// public void warn(Marker marker, CharSequence msg)
//	// public void warn(Marker marker, CharSequence format, Object arg)
//	// public void warn(Marker marker, CharSequence format, Object arg1, Object arg2)
//	// public void warn(Marker marker, CharSequence format, Object... arguments)
//	// public void warn(Marker marker, CharSequence msg, Throwable t)
//	// public boolean isErrorEnabled(Marker marker)
//	// public void error(Marker marker, CharSequence msg)
//	// public void error(Marker marker, CharSequence format, Object arg)
//	// public void error(Marker marker, CharSequence format, Object arg1, Object arg2)
//	// public void error(Marker marker, CharSequence format, Object... arguments)
//	// public void error(Marker marker, CharSequence msg, Throwable t)
//}