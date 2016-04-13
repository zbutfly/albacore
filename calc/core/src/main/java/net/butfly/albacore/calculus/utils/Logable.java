package net.butfly.albacore.calculus.utils;

import java.lang.reflect.Field;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Logable {
	default Logger logger() {
		try {
			Field f = this.getClass().getDeclaredField("logger");
			f.setAccessible(true);
			return (Logger) f.get(this);
		} catch (Throwable e) {
			return LoggerFactory.getLogger(this.getClass());
		}
	};

	default void trace(Supplier<String> func) {
		if (logger().isTraceEnabled()) logger().trace(func.get());
	}

	default void debug(Supplier<String> func) {
		if (logger().isDebugEnabled()) logger().debug(func.get());
	}

	default void info(Supplier<String> func) {
		if (logger().isInfoEnabled()) logger().info(func.get());
	}

	default void warn(Supplier<String> func, Throwable... th) {
		if (logger().isWarnEnabled()) {
			if (th.length > 0) logger().warn(func.get(), th[0]);
			else logger().warn(func.get());
		}
	}

	default void error(Supplier<String> func, Throwable... th) {
		if (logger().isErrorEnabled()) {
			if (th.length > 0) logger().error(func.get(), th[0]);
			else logger().error(func.get());
		}
	}
}
