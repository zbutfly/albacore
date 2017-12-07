package net.butfly.albacore.utils.logger;

public interface Loggable {
	default Logger logger() {
		return Logger.getLogger(getClass());
	}
}
