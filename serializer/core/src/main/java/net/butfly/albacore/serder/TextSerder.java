package net.butfly.albacore.serder;

import java.io.IOException;

import com.google.common.io.CharStreams;

@SuppressWarnings({ "rawtypes" })
public interface TextSerder<PRESENT> extends ContentSerder<PRESENT, CharSequence> {
	default void serialize(Appendable writer, Object src) throws IOException {
		writer.append(this.serialize(src));
	}

	default Object deserialize(Readable reader, Class to) throws IOException {
		return deserialize(CharStreams.toString(reader), to);
	}

	default byte[] toBytes(Object src) {
		return serialize(src).toString().getBytes(contentType().getCharset());
	}

	default Object fromBytes(byte[] bytes, Class to) {
		return deserialize(new String(bytes, contentType().getCharset()), to);
	}
}
