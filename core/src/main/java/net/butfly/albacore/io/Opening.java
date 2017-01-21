package net.butfly.albacore.io;

import net.butfly.albacore.utils.logger.Loggable;

public interface Opening extends Loggable, Named {
	default void opening() {
		logger().debug(name() + " opening...");
	}

	default void closing() {
		logger().debug(name() + " closing...");
	}
}
