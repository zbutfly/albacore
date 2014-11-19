package net.butfly.albacore.cache.utils.tool;

import org.apache.commons.codec.digest.DigestUtils;

public final class MD5Encrypt {
	private static final String MD5_PREFIX = "www.cmbchina.com";
	private static final ThreadLocal<MD5Encrypt> local = new ThreadLocal<MD5Encrypt>();

	private MD5Encrypt() {
		super();
	}

	public static MD5Encrypt getEncrypt() {
		MD5Encrypt encrypt = local.get();
		if (encrypt == null) {
			encrypt = new MD5Encrypt();
			local.set(encrypt);
		}
		return encrypt;
	}

	@Deprecated
	public String encode(String s) {
		if (s == null) { return null; }
		return DigestUtils.md5Hex(MD5_PREFIX + s);
	}

	public String encode(byte[] s) {
		if (s == null) { return null; }
		return DigestUtils.md5Hex(s);
	}
}
