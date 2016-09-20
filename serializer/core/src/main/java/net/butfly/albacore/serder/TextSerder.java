package net.butfly.albacore.serder;

import java.io.IOException;

import com.google.common.io.CharStreams;

public interface TextSerder<PRESENT> extends ContentSerder<PRESENT, CharSequence> {
	@Override
	<T extends PRESENT> String ser(T from);

	default <T extends PRESENT> void ser(Appendable writer, T src) throws IOException {
		writer.append(ser(src));
	}

	default <T extends PRESENT> T der(Readable reader, Class<T> to) throws IOException {
		return der(CharStreams.toString(reader), to);
	}

	default <T extends PRESENT> byte[] toBytes(T src) {
		return ser(src).getBytes(contentType().getCharset());
	}

	default <T extends PRESENT> T fromBytes(byte[] from, Class<T> to) {
		return der(new String(from, contentType().getCharset()), to);
	}
}
