package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Callable<V> extends java.util.concurrent.Callable<V>, Serializable {
	/**
	 * Computes a result, or throws an exception if unable to do so.
	 *
	 * @return computed result
	 * @throws Exception
	 *             if unable to compute a result
	 */
	@Override
	V call() throws Exception;
}
