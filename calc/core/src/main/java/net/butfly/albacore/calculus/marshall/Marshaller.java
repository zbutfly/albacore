package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;

public interface Marshaller<V, K> extends Serializable {
	String unmarshallId(K id);

	<T extends Functor<T>> T unmarshall(V from, Class<T> to);

	K marshallId(String id);

	<T extends Functor<T>> V marshall(T from);

	<F extends Functor<F>> void confirm(Class<F> functorClass, DataSource ds, Detail detail);
}
