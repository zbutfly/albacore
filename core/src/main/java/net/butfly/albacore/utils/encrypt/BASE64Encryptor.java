package net.butfly.albacore.utils.encrypt;

import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

public class BASE64Encryptor extends Encryptor {
	private static Encoder encoder = Base64.getEncoder();
	private static Decoder decoder = Base64.getDecoder();

	@Override
	public String encrypt(String plain) {
		return encoder.encodeToString(plain.getBytes());
	}

	@Override
	public String decrypt(String cipher) {
		return new String(decoder.decode(cipher));
	}

	@Override
	public byte[] encrypt(byte[] plain) {
		return encoder.encode(plain);
	}

	@Override
	public byte[] decrypt(byte[] cipher) {
		return decoder.decode(cipher);
	}

	static public String encode(byte[] data) {
		return encoder.encodeToString(data);
	}

	static public byte[] decode(String str) {
		return decoder.decode(str);
	}
}
