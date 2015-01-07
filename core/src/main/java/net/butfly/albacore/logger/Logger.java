package net.butfly.albacore.logger;

import org.slf4j.helpers.MessageFormatter;

@Deprecated
public class Logger {
	private org.slf4j.Logger logger;
	private org.apache.commons.logging.Log fatalLogger;

	public Logger(Class<?> clazz) {
		this.logger = org.slf4j.LoggerFactory.getLogger(clazz);
		this.fatalLogger = org.apache.commons.logging.LogFactory.getLog(clazz);
	}

	public void debug(String msg) {
		logger.debug(msg);
	}

	public void debug(String msg, Object... args) {
		logger.debug(msg, args);
	}

	public void debug(String msg, Throwable thr, Object... args) {
		logger.debug(format(msg, args), thr);
	}

	public void info(String msg) {
		logger.info(msg);
	}

	public void info(String msg, Object[] args) {
		logger.info(msg, args);
	}

	public void info(String msg, Throwable thr, Object... args) {
		logger.info(format(msg, args), thr);
	}

	public void warn(String msg) {
		logger.warn(msg);
	}

	public void warn(String msg, Object[] args) {
		logger.warn(msg, args);
	}

	public void warn(String msg, Throwable thr, Object... args) {
		logger.warn(format(msg, args), thr);
	}

	public void error(String msg) {
		logger.error(msg);
	}

	public void error(String msg, Object[] args) {
		logger.error(msg, args);
	}

	public void error(String msg, Throwable thr, Object... args) {
		logger.error(format(msg, args), thr);
	}

	public void fatal(String msg) {
		fatalLogger.fatal(msg);
	}

	public void fatal(String msg, Object[] args) {
		fatalLogger.fatal(format(msg, args));
	}

	public void fatal(String msg, Throwable thr, Object... args) {
		fatalLogger.fatal(format(msg, args), thr);
	}

	private String format(String msg, Object... args) {
		return MessageFormatter.format(msg, args).getMessage();
	}
}
