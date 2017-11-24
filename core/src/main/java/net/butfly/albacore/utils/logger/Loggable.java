package net.butfly.albacore.utils.logger;

import java.util.Map;

import net.butfly.albacore.utils.collection.Maps;

public interface Loggable {
	final static Map<Class<? extends Loggable>, Logger> LOGGERS = Maps.of();

	default Logger logger() {
		return LOGGERS.computeIfAbsent(this.getClass(), c -> Logger.getLogger(c));
	}
}
