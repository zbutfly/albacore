package net.butfly.albacore.utils.encrypt;

import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

public class BASE64Encryptor extends Encryptor {
	private Encoder encoder;
	private Decoder decoder;

	public BASE64Encryptor() {
		this.encoder = Base64.getEncoder();
		this.decoder = Base64.getDecoder();
	}

	@Override
	public String encrypt(String plain) {
		return this.encoder.encodeToString(plain.getBytes());
	}

	@Override
	public String decrypt(String cipher) {
		return new String(this.decoder.decode(cipher));
	}

	@Override
	public byte[] encrypt(byte[] plain) {
		return this.encoder.encode(plain);
	}

	@Override
	public byte[] decrypt(byte[] cipher) {
		return this.decoder.decode(cipher);
	}
}
