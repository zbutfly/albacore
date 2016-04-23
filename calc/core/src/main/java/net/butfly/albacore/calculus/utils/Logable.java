package net.butfly.albacore.calculus.utils;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.lambda.Func0;

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

	default void trace(Func0<String> func) {
		if (logger().isTraceEnabled()) logger().trace(func.call());
	}

	default void debug(Func0<String> func) {
		if (logger().isDebugEnabled()) logger().debug(func.call());
	}

	default void info(Func0<String> func) {
		if (logger().isInfoEnabled()) logger().info(func.call());
	}

	default void warn(Func0<String> func, Throwable... th) {
		if (logger().isWarnEnabled()) {
			if (th.length > 0) logger().warn(func.call(), th[0]);
			else logger().warn(func.call());
		}
	}

	default void error(Func0<String> func, Throwable... th) {
		if (logger().isErrorEnabled()) {
			if (th.length > 0) logger().error(func.call(), th[0]);
			else logger().error(func.call());
		}
	}
}
