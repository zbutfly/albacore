package net.butfly.albacore.helper;

interface LoggerHelper extends Helper {
	void debug(String msg);

	void debug(String msg, Object... args);

	void debug(String msg, Throwable thr, Object... args);

	void info(String msg);

	void info(String msg, Object[] args);

	void info(String msg, Throwable thr, Object... args);

	void warn(String msg);

	void warn(String msg, Object[] args);

	void warn(String msg, Throwable thr, Object... args);

	void error(String msg);

	void error(String msg, Object[] args);

	void error(String msg, Throwable thr, Object... args);

	void fatal(String msg);

	void fatal(String msg, Object[] args);

	void fatal(String msg, Throwable thr, Object... args);
}
