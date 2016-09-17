package net.butfly.albacore.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public interface BinarySerializer extends Serializer<byte[]> {
	void serialize(OutputStream out, Serializable src) throws IOException;

	Object deserialize(InputStream in, Class<? extends Serializable> srcClass) throws IOException;
}
