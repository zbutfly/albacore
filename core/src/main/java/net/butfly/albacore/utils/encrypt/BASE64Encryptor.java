package net.butfly.albacore.utils.encrypt;

import java.io.IOException;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class BASE64Encryptor extends Encryptor {
	private BASE64Encoder encoder;
	private BASE64Decoder decoder;

	public BASE64Encryptor() {
		this.encoder = new BASE64Encoder();
		this.decoder = new BASE64Decoder();
	}

	@Override
	public String encrypt(String plain) {
		return this.encoder.encode(plain.getBytes());
	}

	@Override
	public String decrypt(String cipher) {
		try {
			return new String(this.decoder.decodeBuffer(cipher));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public byte[] encrypt(byte[] plain) {
		return this.encoder.encode(plain).getBytes();
	}

	@Override
	public byte[] decrypt(byte[] cipher) {
		try {
			return this.decoder.decodeBuffer(new String(cipher));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
