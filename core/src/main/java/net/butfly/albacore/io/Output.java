package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface Output<D> extends Serializable, Closeable {
	@SuppressWarnings("unchecked")
	void writing(D... data);

	@Override
	default void close() throws IOException {}
}
