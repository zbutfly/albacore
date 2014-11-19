package net.butfly.albacore.config;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import net.butfly.albacore.utils.EncryptUtils;
import net.butfly.albacore.utils.Encryptor;
import net.butfly.albacore.utils.Encryptor.Algorithm;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

public class EncryptedPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

	private static final String DEFAULT_KEY = "com.cmb.ewin";
	private final static String EN_PROP_SUFFIX = ".encrypted";
	private static final Encryptor ENCRYPTOR = new Encryptor(Algorithm.DES, DEFAULT_KEY);

	protected void loadProperties(Properties props) throws IOException {
		super.loadProperties(props);
		Enumeration<?> ke = props.propertyNames();
		while (ke.hasMoreElements()) {
			String k = (String) ke.nextElement();
			if (k.endsWith(EN_PROP_SUFFIX)) {
				String kk = k.substring(0, k.length() - EN_PROP_SUFFIX.length());
				if (!"".equals(kk)) {
					props.put(kk, EncryptUtils.decrypt(props.getProperty(k), ENCRYPTOR));
				}
			}
		}
	}
}
