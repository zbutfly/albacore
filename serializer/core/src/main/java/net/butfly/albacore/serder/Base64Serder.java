package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;

import com.google.common.reflect.TypeToken;

public class Base64Serder extends TextSerderBase<ByteArrayOutputStream> implements TextSerder<ByteArrayOutputStream> {
	private static final long serialVersionUID = 1L;

	@Override
	public String ser(ByteArrayOutputStream from) {
		return Base64.getEncoder().encodeToString(from.toByteArray());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends ByteArrayOutputStream> T der(CharSequence from, TypeToken<T> to) {
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		try {
			bao.write(Base64.getDecoder().decode(from.toString()));
		} catch (IOException e) {
			return null;
		}
		return (T) bao;
	}
}
