package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface Input<D> extends Serializable, Closeable {
	List<D> read();

	void commit();

	@Override
	default void close() throws IOException {}
}
