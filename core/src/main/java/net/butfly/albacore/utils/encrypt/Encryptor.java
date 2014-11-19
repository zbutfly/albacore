package net.butfly.albacore.utils.encrypt;

import java.nio.charset.Charset;

import net.butfly.albacore.utils.ByteUtils;

public abstract class Encryptor {
	private Charset charset = Charset.forName("UTF-8");

	public String encrypt(String plain) {
		return ByteUtils.byte2hex(this.encrypt(plain.getBytes(charset)));
	}

	public String decrypt(String cipher) {
		return new String(this.decrypt(ByteUtils.hex2byte(cipher)), charset);
	}

	public abstract byte[] encrypt(byte[] plain);

	public abstract byte[] decrypt(byte[] cipher);

	public void setCharset(String charset) {
		this.charset = Charset.forName(charset);
	}
}
