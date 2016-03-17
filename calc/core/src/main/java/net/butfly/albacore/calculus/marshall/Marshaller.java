package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.functor.Functor;

@SuppressWarnings("unchecked")
public abstract class Marshaller<V, K> implements Serializable {
	private static final long serialVersionUID = 6678021328832491260L;

	public String unmarshallId(K id) {
		return null == id ? null : id.toString();
	}

	public <T extends Functor<T>> T unmarshall(V from, Class<T> to) {
		return (T) from;
	}

	public K marshallId(String id) {
		return (K) id;
	}

	public <T extends Functor<T>> V marshall(T from) {
		return (V) from;
	}

	public <F extends Functor<F>> boolean confirm(Class<F> functorClass, DataSource ds, Detail detail) {
		return true;
	}
}
