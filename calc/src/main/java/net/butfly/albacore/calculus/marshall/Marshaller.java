package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import net.butfly.albacore.calculus.Functor;

public interface Marshaller<V> {
	<T extends Functor<T>> T unmarshall(V from, Class<Functor<T>> to);

	<T extends Functor<T>> V marshall(T from);
}
