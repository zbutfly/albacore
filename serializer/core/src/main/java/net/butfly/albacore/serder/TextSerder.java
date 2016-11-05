package net.butfly.albacore.serder;

import java.io.IOException;
import java.nio.charset.Charset;

import com.google.common.io.CharStreams;

public interface TextSerder<PRESENT> extends Serder<PRESENT, CharSequence> {
	default <T extends PRESENT> void ser(Appendable writer, T src) throws IOException {
		writer.append(ser(src));
	}

	default <T extends PRESENT> T der(Readable reader, Class<T> to) throws IOException {
		return der(CharStreams.toString(reader), to);
	}

	default <T extends PRESENT> byte[] toBytes(T src, Class<T> to) {
		CharSequence s = ser(src);
		return null == s ? null : s.toString().getBytes(charset());
	}

	default <T extends PRESENT> T fromBytes(byte[] from, Class<T> to) {
		return null == from ? null : der(new String(from, charset()), to);
	}

	default Charset charset() {
		return Charset.defaultCharset();
	}
}
