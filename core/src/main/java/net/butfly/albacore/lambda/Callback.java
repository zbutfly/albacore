package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Callback<V> extends Consumer<V>, Serializable {
	/**
	 * Computes a result, or throws an exception if unable to do so.
	 *
	 * @return computed result
	 * @throws Exception
	 *             if unable to compute a result
	 */
	void call(V v) throws Exception;

	@Override
	default void accept(V v) {
		RunnableEx r = () -> call(v);
		r.runEx();
	}
}
