package net.butfly.albacore.utils.encrypt;

import net.butfly.albacore.utils.encrypt.Algorithm.CipherAlgorithm;
import net.butfly.albacore.utils.encrypt.Algorithm.DigesterAlgorithm;

public final class EncryptFactory {
	@SuppressWarnings("unchecked")
	public static <E extends Encryptor> E getEncryptor(Algorithm algorithm) {
		if (null == algorithm) return (E) new BASE64Encryptor();
		if (algorithm instanceof DigesterAlgorithm) return (E) new DigesterEncryptor((DigesterAlgorithm) algorithm);
		if (algorithm instanceof CipherAlgorithm) return (E) new CipherEncryptor((CipherAlgorithm) algorithm);
		return (E) new BASE64Encryptor();
	}
}
