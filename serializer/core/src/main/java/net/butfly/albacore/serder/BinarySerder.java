package net.butfly.albacore.serder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.reflect.TypeToken;

public interface BinarySerder<PRESENT> extends ContentSerder<PRESENT, byte[]> {
	<T> void ser(T from, OutputStream to) throws IOException;

	<T> T der(InputStream from, TypeToken<T> to) throws IOException;
}
