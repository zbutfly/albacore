package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.butfly.albacore.utils.logger.Logger;

public final class IOs extends Utils {
	private static final Logger logger = Logger.getLogger(IOs.class);

	public static Properties loadAsProps(String classpathPropsFile) {
		Properties props = new Properties();
		InputStream ips = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathPropsFile);
		if (null != ips) try {
			props.load(ips);
		} catch (IOException e) {
			logger.error("Properties file " + classpathPropsFile + " loading failure", e);
		}
		return props;
	}

	public static String config(String key, String defaultValue, Properties... props) {
		String value = System.getenv(key);
		if (null != value) return value;
		for (Properties p : props)
			if (null != p && p.containsKey(key)) return p.getProperty(key);
		return defaultValue;
	}
}
