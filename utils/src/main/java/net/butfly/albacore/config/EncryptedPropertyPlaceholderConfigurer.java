package net.butfly.albacore.config;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import net.butfly.albacore.utils.encrypt.Algorithm.CipherAlgorithm;
import net.butfly.albacore.utils.encrypt.CipherEncryptor;
import net.butfly.albacore.utils.encrypt.EncryptFactory;
import net.butfly.albacore.utils.encrypt.EncryptUtils;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

public class EncryptedPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {
	private String encryptedSuffix = ".encrypted";
	private final CipherEncryptor encryptor = EncryptFactory.getEncryptor(CipherAlgorithm.DES);

	public void setKey(String key) {
		this.encryptor.setKey(key);
	}

	protected void loadProperties(Properties props) throws IOException {
		super.loadProperties(props);
		Enumeration<?> ke = props.propertyNames();
		while (ke.hasMoreElements()) {
			String k = (String) ke.nextElement();
			if (k.endsWith(encryptedSuffix)) {
				String kk = k.substring(0, k.length() - encryptedSuffix.length());
				if (!"".equals(kk)) {
					props.put(kk, EncryptUtils.decrypt(props.getProperty(k), encryptor));
				}
			}
		}
	}

	public void setEncryptedSuffix(String encryptedSuffix) {
		this.encryptedSuffix = encryptedSuffix;
	}
}
