package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("unchecked")
public interface Output<F> extends Serializable, Closeable {
	void write(F dest, Map<String, Object>... data);

	<E> void write(F dest, E... data);

	@Override
	default void close() throws IOException {}
}
