package net.butfly.albacore.utils.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

public interface Serializer {
	boolean supportHTTPStream();

	void write(Writer writer, Object obj) throws IOException;

	void write(OutputStream os, Object obj) throws IOException;

	<T> T read(Reader reader, Class<?>... types) throws IOException;

	<T> T read(InputStream is, Class<?>... types) throws IOException;

	void read(Reader reader, Writer writer, Class<?>... type) throws IOException;

	void read(InputStream is, OutputStream os, Class<?>... type) throws IOException;
}
