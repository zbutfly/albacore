package net.butfly.albacore.utils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;

import net.butfly.albacore.exception.SystemException;

public class DESEncryptUtils {
	public static final String DEFAULT_KEY = "ebase.helper.config";

	private static final String CIPHER_DESC = "DES/CBC/PKCS5Padding";

	private static final String ENCODING = "UTF-8";

	private static final String ALGORITHM = "DES";

	private DESEncryptUtils() {}

	public static String encrypt(String message, String key) {
		try {
			if (null == key) {
				key = DEFAULT_KEY;
			}
			if (key.length() > 8) {
				key = key.substring(0, 8);
			}

			Cipher cipher = Cipher.getInstance(CIPHER_DESC);

			DESKeySpec desKeySpec = new DESKeySpec(key.getBytes(ENCODING));

			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
			// 密钥
			SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
			// 偏移量
			IvParameterSpec iv = new IvParameterSpec(key.getBytes(ENCODING));
			cipher.init(Cipher.ENCRYPT_MODE, secretKey, iv);
			return ByteUtils.byte2hex(cipher.doFinal(message.getBytes(ENCODING))).toUpperCase();
		} catch (Exception e) {
			throw new SystemException("");
		}
	}

	public static String decrypt(String message, String key) {
		try {
			if (null == key) {
				key = DEFAULT_KEY;
			}
			if (key.length() > 8) {
				key = key.substring(0, 8);
			}
			byte[] bytesrc = ByteUtils.hex2byte(message);
			Cipher cipher = Cipher.getInstance(CIPHER_DESC);
			DESKeySpec desKeySpec = new DESKeySpec(key.getBytes(ENCODING));
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
			SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
			IvParameterSpec iv = new IvParameterSpec(key.getBytes(ENCODING));

			cipher.init(Cipher.DECRYPT_MODE, secretKey, iv);

			byte[] retByte = cipher.doFinal(bytesrc);
			return new String(retByte);
		} catch (Exception e) {
			throw new SystemException("");
		}

	}

	public static void main(String[] args) throws Exception {
		String str = DESEncryptUtils.encrypt("test", DEFAULT_KEY);
		System.out.println("encodding cmbChinA：" + str);
		System.out.println("discodding" + str + "：" + DESEncryptUtils.decrypt(str, DEFAULT_KEY));

	}
}
