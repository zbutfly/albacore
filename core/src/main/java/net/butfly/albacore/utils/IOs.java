package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import net.butfly.albacore.utils.logger.Logger;

public final class IOs extends Utils {
	private static final Logger logger = Logger.getLogger(IOs.class);

	public static void writeInteger(OutputStream out, int i) throws IOException {
		out.write(i & 0x000000FF);
		out.write((i >> 8) & 0x000000FF);
		out.write((i >> 16) & 0x000000FF);
		out.write((i >> 24) & 0x000000FF);
	}

	public static int readInteger(InputStream in) throws IOException {
		int i = in.read();
		i |= (in.read() << 8);
		i |= (in.read() << 16);
		i |= (in.read() << 24);
		return i;
	}

	public static void writeBytes(OutputStream out, byte[]... bytes) throws IOException {
		for (byte[] b : bytes) {
			if (null == b) writeInteger(out, -1);
			writeInteger(out, b.length);
			if (b.length > 0) out.write(b);
		}
	}

	public static byte[] readBytes(InputStream in) throws IOException {
		int l = readInteger(in);
		if (l < 0) return null;
		if (l == 0) return new byte[0];
		byte[] bytes = new byte[l];
		int r = in.read(bytes);
		while (r < l && l > 0) {
			// not full read but no data remained.
			if (r == -1) throw new IOException();
			l -= r;
			r = in.read(bytes, r, l);
		}
		return bytes;
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

	public static String config(String key, String defaultValue, Properties... props) {
		String value = System.getenv(key);
		if (null != value) return value;
		for (Properties p : props)
			if (null != p && p.containsKey(key)) return p.getProperty(key);
		return defaultValue;
	}
}
