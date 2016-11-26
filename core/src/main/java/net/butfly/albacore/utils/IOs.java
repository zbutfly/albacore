package net.butfly.albacore.utils;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

	public static InputStream loadJavaFile(String file) {
		try {
			return new FileInputStream(file);
		} catch (FileNotFoundException e) {
			return Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
		}
	}

	@Deprecated
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

	public static byte[] readAll(final InputStream is) {
		Reflections.noneNull("null byte array not allow", is);
		try (ByteArrayOutputStream os = new ByteArrayOutputStream();) {
			byte[] buffer = new byte[1024];
			int n;
			while (-1 != (n = is.read(buffer)))
				os.write(buffer, 0, n);
			return os.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
