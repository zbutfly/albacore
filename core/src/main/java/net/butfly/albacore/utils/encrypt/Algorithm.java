package net.butfly.albacore.utils.encrypt;

public interface Algorithm {
	public enum DigesterAlgorithm implements Algorithm {
		MD2("MD2"), MD5("MD5"), SHA1("SHA-1"), SHA224("SHA-224"), SHA256("SHA-256"), SHA384("SHA-384"), SHA512("SHA-512");
		private String name;

		private DigesterAlgorithm(String name) {
			this.name = name;
		}

		public String code() {
			return name;
		}
	}

	public enum CipherAlgorithm implements Algorithm {
		AES, AESWrap, ARCFOUR, Blowfish, DES, DESede, DESedeWrap, ECIES, RC2, RC4, RC5, RSA
	}
}
