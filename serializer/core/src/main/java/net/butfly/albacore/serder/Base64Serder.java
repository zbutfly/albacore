package net.butfly.albacore.serder;

import java.util.Base64;

public class Base64Serder extends TextSerderBase<byte[]> implements TextSerder<byte[]> {
	private static final long serialVersionUID = 1L;

	@Override
	public String ser(byte[] from) {
		return Base64.getEncoder().encodeToString(from);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T der(CharSequence from, Class<T> to) {
		return (T) Base64.getDecoder().decode(from.toString());
	}
}
