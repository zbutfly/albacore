package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import net.butfly.albacore.utils.logger.Logger;

public class Configs extends Utils {
	private static final Logger logger = Logger.getLogger(Configs.class);

	public static Properties read() throws IOException {
		return read((InputStream) null);
	}

	public static Properties read(String file) throws IOException {
		logger.info("Load configurations from: " + file);
		try (InputStream in = IOs.loadJavaFile(file);) {
			return read(in);
		}
	}

	public static Properties read(InputStream in) throws IOException {
		Properties confs = new Properties();
		if (null != in) confs.load(in);
		for (Entry<Object, Object> e : System.getProperties().entrySet())
			if (!filter(e.getKey().toString())) confs.putIfAbsent((String) e.getKey(), (String) e.getValue());
		for (Entry<String, String> e : System.getenv().entrySet()) {
			String key = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, e.getKey());
			if (!filter(key)) confs.putIfAbsent(key, e.getValue());
		}
		logger.trace("Configuration loaded: " + confs.toString());
		return confs;
	}

	private static String[] IGNORE_PREFIXS = new String[] { "java", "sun", "os", "user", "file", "path" };

	private static boolean filter(String key) {
		for (String ig : IGNORE_PREFIXS)
			if (ig.equals(key) || key.startsWith(ig)) return true;
		return (key.charAt(0) >= 'A' && key.charAt(0) <= 'Z');
	}
}
