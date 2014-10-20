package net.butfly.albacore.utils.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer {
	void write(OutputStream os, Object obj) throws IOException;

	<T> T read(InputStream is, Class<?>... types) throws IOException;

	void readThenWrite(InputStream is, OutputStream os, Class<?>... type) throws IOException;
}
