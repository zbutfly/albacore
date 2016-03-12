package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.FunctorConfig.Detail;
import net.butfly.albacore.calculus.datasource.DataSource;

public interface Marshaller<V, K> extends Serializable {
	String unmarshallId(K id);

	<T extends Functor<T>> T unmarshall(V from, Class<T> to);

	K marshallId(String id);

	<T extends Functor<T>> V marshall(T from);

	<F extends Functor<F>> void confirm(Class<F> functorClass, DataSource ds, Detail detail);

	static Marshaller<?, ?> parseMarshaller(Class<? extends Functor<?>> functorClass, DataSource ds) {
		Stocking s = functorClass.getAnnotation(Stocking.class);
		if (null == s) return null;
		Class<? extends Marshaller<?, ?>> mc = s.marshaller();
		try {
			return DefaultMarshaller.class.equals(mc) ? ds.getMarshaller() : mc.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
