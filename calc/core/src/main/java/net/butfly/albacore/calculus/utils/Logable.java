package net.butfly.albacore.calculus.utils;

import java.io.Serializable;
import java.util.function.Supplier;

import org.slf4j.Logger;

public interface Logable extends Serializable {
	Logger logger();

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
		if (logger().isWarnEnabled()) logger().warn(func.get());
	}

	default void error(Supplier<String> func, Throwable... th) {
		if (logger().isErrorEnabled()) logger().warn(func.get());
	}
}
