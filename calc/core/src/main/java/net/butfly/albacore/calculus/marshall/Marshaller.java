package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.functor.Functor;

public interface Marshaller<V, K> extends Serializable {
	String unmarshallId(K id);

	<T extends Functor<T>> T unmarshall(V from, Class<T> to);

	K marshallId(String id);

	<T extends Functor<T>> V marshall(T from);

	default <F extends Functor<F>> boolean confirm(Class<F> functorClass, DataSource ds, Detail detail) {
		return true;
	}
}
