package net.butfly.albacore.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface BinarySerializer extends ContentSerializer<byte[]> {
	void serialize(OutputStream out, Object src) throws IOException;

	@SuppressWarnings("rawtypes")
	Object deserialize(InputStream in, Class srcClass) throws IOException;
}
