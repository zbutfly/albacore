package net.butfly.albacore.io.deprecated;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

@Deprecated
public interface Input<D> extends Serializable, Closeable {
	List<D> reading();

	void commit();

	@Override
	default void close() throws IOException {}
}
