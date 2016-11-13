package net.butfly.albacore.utils;

import java.io.FileInputStream;
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
		try (InputStream cpis = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);) {
			if (cpis != null) return read(cpis);
			try (InputStream absis = new FileInputStream(file);) {
				return read(absis);
			}
		}
	}

	public static Properties read(InputStream in) throws IOException {
		Properties confs = new Properties();
		if (null != in) confs.load(in);
		for (Entry<Object, Object> e : System.getProperties().entrySet())
			if (!filter(e.getKey().toString())) confs.putIfAbsent((String) e.getKey(), (String) e.getValue());
		for (Entry<String, String> e : System.getenv().entrySet())
			if (!filter(e.getKey())) confs.putIfAbsent(e.getKey(), e.getValue());;
		logger.trace("Configuration loaded: " + confs.toString());
		return confs;
	}

	private static boolean filter(String key) {
		return key.startsWith("java.") || key.startsWith("sun.") || (key.charAt(0) >= 'A' && key.charAt(0) <= 'Z');
	}
}
