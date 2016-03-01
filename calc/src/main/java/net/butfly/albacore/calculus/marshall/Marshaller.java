package net.butfly.albacore.calculus.marshall;

import net.butfly.albacore.calculus.Functor;

public interface Marshaller<V, K> {
	String unmarshallId(K id);

	<T extends Functor<T>> T unmarshall(V from, Class<T> to);

	K marshallId(String id);

	<T extends Functor<T>> V marshall(T from);

	/**
	 * Confirm table of functor loading / saving exist and validate the columns
	 * according to functor class definition (fields, and so on).
	 * 
	 * @param f
	 */
	@SuppressWarnings("rawtypes")
	<F extends Functor> void confirm(Class<F> functor);
}
