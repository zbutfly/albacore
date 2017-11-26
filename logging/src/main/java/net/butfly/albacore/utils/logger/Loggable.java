package net.butfly.albacore.utils.logger;

public interface Loggable {
	default Logger logger() {
		return Logger.loggers.computeIfAbsent(this.getClass().getName(), c -> Logger.getLogger(c));
	}
}
