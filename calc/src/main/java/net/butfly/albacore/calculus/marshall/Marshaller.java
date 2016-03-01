package net.butfly.albacore.calculus.marshall;

import net.butfly.albacore.calculus.CalculatorConfig;
import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.FunctorConfig;

public interface Marshaller<V, K> {
	String unmarshallId(K id);

	<T extends Functor<T>> T unmarshall(V from, Class<T> to);

	K marshallId(String id);

	<T extends Functor<T>> V marshall(T from);

	<F extends Functor<F>> void confirm(Class<F> functor, FunctorConfig config, CalculatorConfig globalConfig);
}
