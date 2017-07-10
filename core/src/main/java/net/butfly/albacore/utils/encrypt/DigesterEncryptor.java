package net.butfly.albacore.utils.encrypt;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import net.butfly.albacore.exception.EncryptException;
import net.butfly.albacore.utils.encrypt.Algorithm.DigesterAlgorithm;

public class DigesterEncryptor extends Encryptor {
	private MessageDigest digester;

	public DigesterEncryptor(DigesterAlgorithm algorithm) {
		try {
			this.digester = MessageDigest.getInstance(algorithm.code());
		} catch (NoSuchAlgorithmException e) {
			throw new EncryptException(e);
		}
	}

	@Override
	public byte[] encrypt(byte[] plain) {
		return this.digester.digest(plain);
	}

	@Override
	public byte[] decrypt(byte[] cipher) {
		throw new UnsupportedOperationException();
	}
}
