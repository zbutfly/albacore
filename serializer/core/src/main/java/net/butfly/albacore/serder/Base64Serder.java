package net.butfly.albacore.serder;

import java.util.Base64;

import net.butfly.albacore.serder.support.ClassInfoSerder;
import net.butfly.albacore.serder.support.TextSerder;

final public class Base64Serder implements TextSerder<byte[]>, ClassInfoSerder<byte[], CharSequence> {
	private static final long serialVersionUID = 1L;

	@Override
	public String ser(byte[] from) {
		return Base64.getEncoder().encodeToString(from);
	}

	@SuppressWarnings("unchecked")
	@Override
	public byte[] der(CharSequence from) {
		return Base64.getDecoder().decode(from.toString());
	}
}
