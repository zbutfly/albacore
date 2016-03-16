package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.functor.Functor;

@SuppressWarnings("unchecked")
public interface Marshaller<V, K> extends Serializable {
	default String unmarshallId(K id) {
		return null == id ? null : id.toString();
	}

	default <T extends Functor<T>> T unmarshall(V from, Class<T> to) {
		return (T) from;
	}

	default K marshallId(String id) {
		return (K) id;
	}

	default <T extends Functor<T>> V marshall(T from) {
		return (V) from;
	}

	default <F extends Functor<F>> boolean confirm(Class<F> functorClass, DataSource ds, Detail detail) {
		return true;
	}
}
