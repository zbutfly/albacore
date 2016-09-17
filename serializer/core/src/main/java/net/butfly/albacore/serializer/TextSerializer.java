package net.butfly.albacore.serializer;

import java.io.IOException;

import com.google.common.io.CharStreams;

@SuppressWarnings({ "rawtypes", "unchecked" })
public interface TextSerializer extends ContentSerializer<CharSequence> {
	default void serialize(Appendable writer, Object src) throws IOException {
		writer.append(this.serialize(src));
	}

	default Object deserialize(Readable reader, Class srcClass) throws IOException {
		return deserialize(CharStreams.toString(reader), srcClass);
	}

	default byte[] toBytes(Object src) {
		return serialize(src).toString().getBytes(contentType().getCharset());
	}

	default Object fromBytes(byte[] bytes, Class srcClass) {
		return deserialize(new String(bytes, contentType().getCharset()), srcClass);
	}
}
