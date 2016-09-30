package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.butfly.albacore.utils.logger.Logger;

public final class IOs extends Utils {
	private static final Logger logger = Logger.getLogger(IOs.class);

	public static boolean debug() {
		return Boolean.parseBoolean(System.getProperty("albacore.debug"));
	}

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
}
