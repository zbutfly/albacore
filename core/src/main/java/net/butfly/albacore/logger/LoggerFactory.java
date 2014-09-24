package net.butfly.albacore.logger;

import java.util.Map;
import java.util.WeakHashMap;

public class LoggerFactory {
	private static final Map<Class<?>, Logger> loggerCache = new WeakHashMap<Class<?>, Logger>();

	private LoggerFactory() {}

	static {
		initialize();
	}

	public static void initialize() {
		Log4jFactory.initializeLog4j();
	}

	public static Logger getLogger(Class<?> clazz) {
		Logger logger = loggerCache.get(clazz);
		if (null == logger) {
			logger = new Logger(clazz);
			loggerCache.put(clazz, logger);
		}
		return logger;
	}
}
