package net.butfly.albacore.utils.logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface Loggable {
	final static Map<Class<? extends Loggable>, Logger> LOGGERS = new ConcurrentHashMap<>();

	default Logger logger() {
		return LOGGERS.computeIfAbsent(this.getClass(), c -> Logger.getLogger(c));
	}
}
