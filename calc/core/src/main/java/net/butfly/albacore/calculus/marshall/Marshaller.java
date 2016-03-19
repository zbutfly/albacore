package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.Detail;
import net.butfly.albacore.calculus.factor.Factor;

@SuppressWarnings("unchecked")
public abstract class Marshaller<V, K> implements Serializable {
	private static final long serialVersionUID = 6678021328832491260L;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public String unmarshallId(K id) {
		return null == id ? null : id.toString();
	}

	public <T extends Factor<T>> T unmarshall(V from, Class<T> to) {
		return (T) from;
	}

	public K marshallId(String id) {
		return (K) id;
	}

	public <T extends Factor<T>> V marshall(T from) {
		return (V) from;
	}

	public <F extends Factor<F>> boolean confirm(Class<F> factorClass, DataSource<K, V> ds, Detail detail) {
		return true;
	}
}
