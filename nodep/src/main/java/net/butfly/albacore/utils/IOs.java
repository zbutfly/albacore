package net.butfly.albacore.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IOs {
	private static final Logger logger = LoggerFactory.getLogger(IOs.class);

	public static InputStream open(String... file) {
		if (file == null || file.length == 0) return null;
		InputStream is = openFile(file);
		if (null != is) return is;
		return openClasspath(file);
	}

	public static InputStream openFile(String... file) {
		if (file == null || file.length == 0) return null;
		for (String f : file)
			try {
				FileInputStream s = new FileInputStream(f);
				if (null != s) return s;
			} catch (Exception e) {}
		return null;
	}

	public static InputStream openClasspath(Class<?> loader, String... file) {
		if (loader == null || file == null || file.length == 0) return null;
		for (String f : file) {
			try {
				InputStream s = loader.getClassLoader().getResourceAsStream(f);
				if (null != s) return s;
			} catch (Exception e) {}
		}
		return null;
	}

	public static InputStream openClasspath(String... file) {
		if (file == null || file.length == 0) return null;
		for (String f : file) {
			try {
				InputStream s = Thread.currentThread().getContextClassLoader().getResourceAsStream(f);
				if (null != s) return s;
			} catch (Exception e) {}
		}
		return null;
	}

	public static <S extends OutputStream> S writeBytes(S out, byte[] b) throws IOException {
		if (null == b) {
			writeInt(out, -1);
			if (logger.isTraceEnabled()) logger.trace("writeBytes: length[-1]");
		} else {
			writeInt(out, b.length);
			if (logger.isTraceEnabled()) logger.trace("writeBytes: length[" + b.length + "]");
			if (b.length > 0) {
				out.write(b);
				if (logger.isTraceEnabled()) logger.trace("writeBytes: data[" + b.length + "]");
			}
		}
		return out;
	}

	public static byte[] readBytes(InputStream in) throws IOException {
		int l = readInt(in);
		if (logger.isTraceEnabled()) logger.trace("readBytes: length[" + l + "]");
		if (l < 0) return null;
		if (l == 0) return new byte[0];
		byte[] bytes = new byte[l];
		for (int i = 0; i < l; i++) {
			int b;
			if ((b = in.read()) < 0)//
				throw new EOFException("not full read but no data remained, need [" + l + ", now [" + i + "]]");
			else bytes[i] = (byte) b;
		}
		if (logger.isTraceEnabled()) logger.trace("readBytes: data[" + l + "]");
		return bytes;
	}

	public static <S extends OutputStream> S writeBytes(S out, byte[]... bytes) throws IOException {
		if (logger.isTraceEnabled()) logger.trace(bytes.length > 1 ? "Write bytes list: " + bytes.length : null);
		writeInt(out, bytes.length);
		for (byte[] b : bytes)
			writeBytes(out, b);
		return out;
	}

	@SafeVarargs
	public static <T, S extends OutputStream> S writeBytes(S out, Function<T, byte[]> ser, T... bytes) throws IOException {
		if (logger.isTraceEnabled()) logger.trace(bytes.length > 1 ? "Write bytes list: " + bytes.length : null);
		writeInt(out, bytes.length);
		for (T b : bytes)
			writeBytes(out, ser.apply(b));
		return out;
	}

	public static byte[][] readBytesList(InputStream in) throws IOException {
		int count = readInt(in);
		if (logger.isTraceEnabled()) logger.trace(count > 1 ? "Read bytes list: " + count : null);
		byte[][] r = new byte[count][];
		for (int i = 0; i < count; i++)
			r[i] = readBytes(in);
		return r;
	}

	public static <T> List<T> readBytes(InputStream in, Function<byte[], T> der) throws IOException {
		int count = readInt(in);
		if (logger.isTraceEnabled()) logger.trace(count > 1 ? "Read bytes list: " + count : null);
		List<T> r = new ArrayList<>();
		for (int i = 0; i < count; i++)
			r.add(der.apply(readBytes(in)));
		return r;
	}

	public static <S extends OutputStream> S writeInt(S out, int i) throws IOException {
		logger.trace("Write int: " + i);
		out.write(i & 0x000000FF);
		i >>= 8;
		out.write(i & 0x000000FF);
		i >>= 8;
		out.write(i & 0x000000FF);
		i >>= 8;
		out.write(i & 0x000000FF);
		return out;
	}

	public static int readInt(InputStream in) throws IOException {
		int b, i = 0, offset = -8;
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if ((b = in.read()) >= 0) i |= b << (offset += 8);
		if (logger.isTraceEnabled()) logger.trace("Read int: " + i);
		return i;
	}

	public static <S extends OutputStream> S writeObj(S out, Object obj) throws IOException {
		if (logger.isTraceEnabled()) logger.trace("Write object: " + (null == obj ? null : obj.getClass().getName()));
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream is = new ObjectOutputStream(baos)) {
			is.writeObject(obj);
			writeBytes(out, baos.toByteArray());
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	public static <T> T readObj(InputStream in) throws IOException {
		byte[] bytes = readBytes(in);
		if (null == bytes || bytes.length == 0) return null;
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); ObjectInputStream is = new ObjectInputStream(bais)) {
			try {
				T obj = (T) is.readObject();
				if (logger.isTraceEnabled()) logger.trace("Read object: " + (null == obj ? null : obj.getClass().getName()));
				return obj;
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
	}

	public static byte[] readAll(final InputStream is) {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream();) {
			byte[] buffer = new byte[1024];
			int n;
			while (-1 != (n = Objects.requireNonNull(is, "null byte array not allow").read(buffer)))
				os.write(buffer, 0, n);
			byte[] b = os.toByteArray();
			if (logger.isTraceEnabled()) logger.trace("Read all: " + b.length);
			return b;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String[] readLines(final InputStream is) {
		return readLines(is, l -> false);
	}

	public static String[] readLines(final InputStream is, Predicate<String> ignore) {
		List<String> lines = new ArrayList<>();
		String l = null;
		try (BufferedReader r = new BufferedReader(new InputStreamReader(Objects.requireNonNull(is, "null byte array not allow")));) {
			while ((l = r.readLine()) != null)
				if (!ignore.test(l)) lines.add(l);
			if (logger.isTraceEnabled()) logger.trace("Read all: " + lines.size());
			return lines.toArray(new String[lines.size()]);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String mkdirs(String path) throws IOException {
		File dir = new File(path + "/");
		if (!dir.exists() || !dir.isDirectory()) dir.mkdirs();
		return dir.getCanonicalPath();
	}

	public static Path mkdirs(Path path) {
		if (Files.exists(path) && Files.isDirectory(path)) return path;
		try {
			return Files.createDirectories(path);
		} catch (IOException e) {
			throw new IllegalArgumentException("Non-existed directory [" + path + "] could not be created.");
		}
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

	public static long bytes2long(byte[] bytes) {
		Objects.requireNonNull(bytes, "Null bytes invalid parsing into long");
		if (bytes.length < 8) bytes = Arrays.copyOf(bytes, 8);
		long l = 0L;
		for (int i = 0; i < 8; i++)
			l += bytes[i] << (i * 8);
		return l;
	}

	public static void flushAndClose(Closeable io) {
		if (null == io) return;
		if (io instanceof Flushable) try {
			((Flushable) io).flush();
		} catch (IOException e) {
			logger.trace("Vfs flushing failed on [" + io.toString() + "].", e);
		}
		try {
			io.close();
		} catch (IOException e) {
			logger.warn("Close fail on [" + io.toString() + "].", e);
		}
	}

	private IOs() {}
}
