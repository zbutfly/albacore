package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import scala.Tuple3;

@SuppressWarnings("unchecked")
public interface Input<F> extends Serializable, Closeable {

	List<Tuple3<String, byte[], Map<String, Object>>> read(F... filters);

	<E> List<Tuple3<String, byte[], E>> read(Class<E> cl, F... filters);

	void commit();

	@Override
	default void close() throws IOException {}
}
