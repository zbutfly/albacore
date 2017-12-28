package net.butfly.albacore.utils.logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class Loggable {
	final static Map<Class<? extends Loggable>, Logger> LOGGERS = new ConcurrentHashMap<Class<? extends Loggable>, Logger>();

	public Logger logger() {
		return LOGGERS.computeIfAbsent(this.getClass(), new Function<Class<? extends Loggable>, Logger>() {
			@Override
			public Logger apply(Class<? extends Loggable> c) {
				return Logger.getLogger(c);
			}
		});
	}
}
