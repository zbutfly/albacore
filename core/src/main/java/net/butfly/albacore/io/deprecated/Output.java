package net.butfly.albacore.io.deprecated;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

@Deprecated
public interface Output<D> extends Serializable, Closeable {
	@SuppressWarnings("unchecked")
	void writing(D... data);

	@Override
	default void close() throws IOException {}
}
