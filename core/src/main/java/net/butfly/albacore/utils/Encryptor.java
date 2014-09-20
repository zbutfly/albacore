package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import net.butfly.albacore.exception.EncryptException;

public final class Encryptor {
	public enum Algorithm {
		DES("DES"), @Deprecated
		DES3("DESede"), @Deprecated
		AES("AES"), @Deprecated
		Blowfish("Blowfish"), @Deprecated
		RC2("RC2"), @Deprecated
		RC4("RC4");

		private String code;

		private Algorithm(String algorithm) {
			this.code = algorithm;
		}

		public static Algorithm parse(String algorithm) throws NoSuchAlgorithmException {
			for (Algorithm a : Algorithm.values()) {
				if (a.code.equals(algorithm)) { return a; }
			}
			throw new NoSuchAlgorithmException();
		}

		public String id() {
			return code;
		}
	}

	private Algorithm algorithm;
	private Cipher[] ciphers = new Cipher[2];

	public Encryptor(Algorithm algorithm, String keySeed) {
		this.algorithm = algorithm;
		try {
			// TODO: create key without algorithm definition.
			Key key = SecretKeyFactory.getInstance(algorithm.id()).generateSecret(new DESKeySpec(keySeed.getBytes()));
			// Key key = new SecretKeySpec(keySeed.getBytes(),
			// this.algorithm.id());
			ciphers[0] = Cipher.getInstance(this.algorithm.id());
			ciphers[0].init(Cipher.ENCRYPT_MODE, key);
			ciphers[1] = Cipher.getInstance(this.algorithm.id());
			ciphers[1].init(Cipher.DECRYPT_MODE, key);
		} catch (GeneralSecurityException e) {
			throw new EncryptException(e);
		}
	}

	public byte[] encrypt(byte[] uncrypted) {
		try {
			byte[] raw = ciphers[0].doFinal(uncrypted);
			return raw;
		} catch (GeneralSecurityException e) {
			throw new EncryptException(e);
		}
	}

	public byte[] decrypt(byte[] encrypted) {
		try {
			return ciphers[1].doFinal(encrypted);
		} catch (GeneralSecurityException e) {
			throw new EncryptException(e);
		}

	}

	public Key random() {
		try {
			// Security.insertProviderAt(new com.ibm.crypto.provider.IBMJCE(),
			// 1);
			// Security.insertProviderAt(new com.sun.crypto.provider.SunJCE(),
			// 1);
			KeyGenerator generator = KeyGenerator.getInstance(algorithm.id());
			generator.init(new SecureRandom());
			return generator.generateKey();
		} catch (NoSuchAlgorithmException e) {
			throw new EncryptException(e);
		}
	}

	public void save(Key key, OutputStream file) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(file);
		oos.writeObject(key);
		oos.close();
	}

	public Key load(InputStream file) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(file);
		return (Key) ois.readObject();
	}
}
