package net.butfly.albacore.helper;

public interface EncryptHelper extends Helper {
	public String encrypt(String src);

	public String decrypt(String src);

	public byte[] encrypt(byte[] src);

	public byte[] decrypt(byte[] src);
}
