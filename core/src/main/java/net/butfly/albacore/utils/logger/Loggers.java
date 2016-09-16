package net.butfly.albacore.utils.logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;
import org.slf4j.impl.Log4jLoggerFactory;

import net.butfly.albacore.utils.Utils;

public final class Loggers extends Utils {
	public static boolean isTraceEnabled() {
		return logger().isTraceEnabled();
	}

	public static void trace(Supplier<String> msg) {
		Logger l = logger();
		if (l.isTraceEnabled()) l.trace(msg.get());
	}

	public static void trace(String format, Supplier<Object[]> args) {
		Logger l = logger();
		if (l.isTraceEnabled()) l.trace(format, args.get());
	}

	public static void trace(Supplier<String> msg, Throwable t) {
		Logger l = logger();
		if (l.isTraceEnabled()) l.trace(msg.get(), t);
	}

	public static void debug(Supplier<String> msg) {
		Logger l = logger();
		if (l.isDebugEnabled()) l.debug(msg.get());
	}

	public static void debug(String format, Supplier<Object[]> args) {
		Logger l = logger();
		if (l.isDebugEnabled()) l.debug(format, args.get());
	}

	public static void debug(Supplier<String> msg, Throwable t) {
		Logger l = logger();
		if (l.isDebugEnabled()) l.debug(msg.get());
	}

	public static void info(Supplier<String> msg) {
		Logger l = logger();
		if (l.isInfoEnabled()) l.info(msg.get());
	}

	public static void info(String format, Supplier<Object[]> args) {
		Logger l = logger();
		if (l.isInfoEnabled()) l.info(format, args.get());
	}

	public static void info(Supplier<String> msg, Throwable t) {
		Logger l = logger();
		if (l.isInfoEnabled()) l.info(msg.get(), t);
	}

	public static void warn(Supplier<String> msg) {
		Logger l = logger();
		if (l.isWarnEnabled()) l.warn(msg.get());
	}

	public static void warn(String format, Supplier<Object[]> args) {
		Logger l = logger();
		if (l.isWarnEnabled()) l.warn(format, args.get());
	}

	public static void warn(Supplier<String> msg, Throwable t) {
		Logger l = logger();
		if (l.isWarnEnabled()) l.warn(msg.get(), t);
	}

	public static void error(Supplier<String> msg) {
		Logger l = logger();
		if (l.isErrorEnabled()) l.error(msg.get());
	}

	public static void error(String format, Supplier<Object[]> args) {
		Logger l = logger();
		if (l.isErrorEnabled()) l.error(format, args.get());
	}

	public static void error(Supplier<String> msg, Throwable t) {
		Logger l = logger();
		if (l.isErrorEnabled()) l.error(msg.get(), t);
	}

	private static Map<String, Logger> loggers = new HashMap<>();

	private static Logger logger() {
		// 0: thread, 1: logger(this method), 2: info/trace/...
		// 3: real class/method
		String c = Thread.currentThread().getStackTrace()[3].getClassName();
		Logger l = loggers.get(c);
		if (null == l) {
			l = LoggerFactory.getLogger(c);
			loggers.put(c, l);
		}
		return l;
	}
}
