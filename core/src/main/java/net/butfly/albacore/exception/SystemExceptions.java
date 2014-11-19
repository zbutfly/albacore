package net.butfly.albacore.exception;

public interface SystemExceptions {
	String _PREFIX = "ABC_";
	String ASYNC_INTERRUPTED = _PREFIX + "080";
	String ASYNC_TIMEOUT = _PREFIX + "081";
	String ASYNC_SATURATED = _PREFIX + "082";
	String UNKNOW_CAUSE = _PREFIX + "999";
}
