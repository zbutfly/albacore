package net.butfly.albacore.serder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface BinarySerder<PRESENT> extends ContentSerder<PRESENT, byte[]> {
	void serialize(OutputStream out, Object src) throws IOException;

	@SuppressWarnings("rawtypes")
	Object deserialize(InputStream in, Class to) throws IOException;
}
