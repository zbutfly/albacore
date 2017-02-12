package net.butfly.albacore.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Properties;

import net.butfly.albacore.utils.logger.Logger;

public final class IOs extends Utils {
	private static final Logger logger = Logger.getLogger(IOs.class);

	private static void writeInteger(OutputStream out, int i) throws IOException {
		out.write(i & 0x000000FF);
		i >>= 8;
		out.write(i & 0x000000FF);
		i >>= 8;
		out.write(i & 0x000000FF);
		i >>= 8;
		out.write(i & 0x000000FF);
	}

	private static int readInteger(InputStream in) throws IOException {
		int b, i = 0, offset = -8;
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		return i;
	}

	public static <S extends OutputStream> S writeBytes(S out, byte[]... bytes) throws IOException {
		for (byte[] b : bytes) {
			if (null == b) writeInteger(out, -1);
			writeInteger(out, b.length);
			if (b.length > 0) out.write(b);
		}
		return out;
	}

	public static byte[] readBytes(InputStream in) throws IOException {
		int l = readInteger(in);
		if (l < 0) return null;
		if (l == 0) return new byte[0];
		byte[] bytes = new byte[l];
		int r;
		while ((r = in.read(bytes)) < l && l > 0)
			// not full read but no data remained.
			if (r == -1) throw new IOException();
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

	public static String mkdirs(String path) throws IOException {
		File dir = new File(path + "/");
		if (!dir.exists() || !dir.isDirectory()) dir.mkdirs();
		return dir.getCanonicalPath();
	}

	public static void main(String... args) throws IOException {
		int i = -8;
		System.out.println(MessageFormat.format("{0},{1},{2},{3}", i += 8, i += 8, i += 8, i += 8));
	}

	public static byte[] toByte(Object obj) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(baos)) {
			out.writeObject(obj);
			return baos.toByteArray();
		} catch (IOException e) {
			logger.error("bytes converting failure", e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromByte(byte[] bytes) {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); ObjectInputStream in = new ObjectInputStream(bais)) {
			return (T) in.readObject();
		} catch (ClassNotFoundException | IOException e) {
			logger.error("bytes converting failure", e);
			return null;
		}
	}
}
