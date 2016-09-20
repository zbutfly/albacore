package net.butfly.albacore.serder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface BinarySerder<PRESENT> extends ContentSerder<PRESENT, byte[]> {
	<T> void ser(T from, OutputStream to) throws IOException;

	<T> T der(InputStream from, Class<T> to) throws IOException;
}
