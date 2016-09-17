package net.butfly.albacore.serializer;

import java.io.IOException;
import java.io.Serializable;

import com.google.common.io.CharStreams;

public interface TextSerializer extends Serializer<CharSequence> {
	default void serialize(Appendable writer, Serializable src) throws IOException {
		writer.append(this.serialize(src));
	}

	default Serializable deserialize(Readable reader, Class<? extends Serializable> srcClass) throws IOException {
		return deserialize(CharStreams.toString(reader), srcClass);
	}

	default byte[] toBytes(Serializable src) {
		return serialize(src).toString().getBytes(contentType().getCharset());
	}

	default Serializable fromBytes(byte[] bytes, Class<? extends Serializable> srcClass) {
		return deserialize(new String(bytes, contentType().getCharset()), srcClass);
	}
}
