package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface RunnableEx extends Serializable {
	void run() throws Exception;

	default Runnable runnable() {
		return this::runEx;
	}

	default void runEx() {
		try {
			run();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void r(RunnableEx r) {
		r.runEx();
	}
}
