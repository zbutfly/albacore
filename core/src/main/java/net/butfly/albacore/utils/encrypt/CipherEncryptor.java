package net.butfly.albacore.utils.encrypt;

import java.security.GeneralSecurityException;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import net.butfly.albacore.exception.EncryptException;
import net.butfly.albacore.utils.encrypt.Algorithm.CipherAlgorithm;

public class CipherEncryptor extends Encryptor {
	private Cipher encryptCipher, decryptCipher;
	private CipherAlgorithm algorithm;

	public CipherEncryptor(CipherAlgorithm algorithm) {
		this.algorithm = algorithm;
	}

	public void setKey(String key) {
		try {
			Key k = SecretKeyFactory.getInstance(algorithm.name()).generateSecret(new DESKeySpec(key.getBytes()));
			this.encryptCipher = Cipher.getInstance(algorithm.name());
			this.encryptCipher.init(Cipher.ENCRYPT_MODE, k);
			this.decryptCipher = Cipher.getInstance(algorithm.name());
			this.decryptCipher.init(Cipher.DECRYPT_MODE, k);
		} catch (GeneralSecurityException e) {
			throw new EncryptException(e);
		}
	}

	@Override
	public byte[] encrypt(byte[] plain) {
		try {
			return this.encryptCipher.doFinal(plain);
		} catch (Exception e) {
			throw new EncryptException(e);
		}
	}

	@Override
	public byte[] decrypt(byte[] cipher) {
		try {
			return this.decryptCipher.doFinal(cipher);
		} catch (Exception e) {
			throw new EncryptException(e);
		}
	}
}
