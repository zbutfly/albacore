package net.butfly.albacore.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import net.butfly.albacore.utils.logger.Logger;

public final class IOs extends Utils {
	private static final Logger logger = Logger.getLogger(IOs.class);

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

	public static <S extends OutputStream> S writeBytes(S out, final byte[] b) throws IOException {
		if (null == b) {
			writeInt(out, -1);
			logger.trace(new Supplier<CharSequence>() {
				@Override
				public CharSequence get() {
					return "writeBytes: length[-1]";
				}
			});
		} else {
			writeInt(out, b.length);
			logger.trace(new Supplier<CharSequence>() {
				@Override
				public CharSequence get() {
					return "writeBytes: length[" + b.length + "]";
				}
			});
			if (b.length > 0) {
				out.write(b);
				logger.trace(new Supplier<CharSequence>() {
					@Override
					public CharSequence get() {
						return "writeBytes: data[" + b.length + "]";
					}
				});
			}
		}
		return out;
	}

	public static byte[] readBytes(InputStream in) throws IOException {
		final int l = readInt(in);
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return "readBytes: length[" + l + "]";
			}
		});
		if (l < 0) return null;
		if (l == 0) return new byte[0];
		byte[] bytes = new byte[l];
		for (int i = 0; i < l; i++) {
			int b;
			if ((b = in.read()) < 0)//
				throw new EOFException("not full read but no data remained, need [" + l + ", now [" + i + "]]");
			else bytes[i] = (byte) b;
		}
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return "readBytes: data[" + l + "]";
			}
		});
		return bytes;
	}

	public static <S extends OutputStream> S writeBytes(S out, final byte[]... bytes) throws IOException {
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return bytes.length > 1 ? "Write bytes list: " + bytes.length : null;
			}
		});
		writeInt(out, bytes.length);
		for (byte[] b : bytes)
			writeBytes(out, b);
		return out;
	}

	@SafeVarargs
	public static <T, S extends OutputStream> S writeBytes(S out, Function<T, byte[]> ser, final T... bytes) throws IOException {
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return bytes.length > 1 ? "Write bytes list: " + bytes.length : null;
			}
		});
		writeInt(out, bytes.length);
		for (T b : bytes)
			writeBytes(out, ser.apply(b));
		return out;
	}

	public static byte[][] readBytesList(InputStream in) throws IOException {
		final int count = readInt(in);
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return count > 1 ? "Read bytes list: " + count : null;
			}
		});
		byte[][] r = new byte[count][];
		for (int i = 0; i < count; i++)
			r[i] = readBytes(in);
		return r;
	}

	public static <T> List<T> readBytes(InputStream in, Function<byte[], T> der) throws IOException {
		final int count = readInt(in);
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return count > 1 ? "Read bytes list: " + count : null;
			}
		});
		List<T> r = new ArrayList<T>();
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
		logger.trace("Read int: " + i);
		return i;
	}

	public static <S extends OutputStream> S writeObj(S out, final Object obj) throws IOException {
		logger.trace(new Supplier<CharSequence>() {
			@Override
			public CharSequence get() {
				return "Write object: " + (null == obj ? null : obj.getClass().getName());
			}
		});
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream is = new ObjectOutputStream(baos);
		try {
			is.writeObject(obj);
			writeBytes(out, baos.toByteArray());
		} finally {
			is.close();
			baos.close();
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	public static <T> T readObj(InputStream in) throws IOException {
		byte[] bytes = readBytes(in);
		if (null == bytes || bytes.length == 0) return null;
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream is = new ObjectInputStream(bais);
		try {
			final T obj = (T) is.readObject();
			logger.trace(new Supplier<CharSequence>() {
				@Override
				public CharSequence get() {
					return "Read object: " + (null == obj ? null : obj.getClass().getName());
				}
			});
			return obj;
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} finally {
			is.close();
			bais.close();
		}
	}

	public static byte[] readAll(final InputStream is) {
		Reflections.noneNull("null byte array not allow", is);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			byte[] buffer = new byte[1024];
			int n;
			while (-1 != (n = is.read(buffer)))
				os.write(buffer, 0, n);
			final byte[] b = os.toByteArray();
			logger.trace(new Supplier<CharSequence>() {
				@Override
				public CharSequence get() {
					return "Read all: " + b.length;
				}
			});
			return b;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String[] readLines(final InputStream is) {
		return readLines(is, new Predicate<String>() {
			@Override
			public boolean test(String l) {
				return false;
			}
		});
	}

	public static String[] readLines(final InputStream is, Predicate<String> ignore) {
		Reflections.noneNull("null byte array not allow", is);
		final List<String> lines = new ArrayList<String>();
		String l = null;
		BufferedReader r = new BufferedReader(new InputStreamReader(is));
		try {
			while ((l = r.readLine()) != null)
				if (!ignore.test(l)) lines.add(l);
			logger.trace(new Supplier<CharSequence>() {
				@Override
				public CharSequence get() {
					return "Read all: " + lines.size();
				}
			});
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
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(baos);
			try {
				out.writeObject(obj);
				return baos.toByteArray();
			} catch (IOException e) {
				logger.error("bytes converting failure", e);
				return null;
			} finally {
				out.close();
				baos.close();
			}
		} catch (IOException e1) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromByte(byte[] bytes) {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream in;
		try {
			in = new ObjectInputStream(bais);
		} catch (IOException e1) {
			return null;
		}
		try {
			return (T) in.readObject();
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		} finally {
			try {
				in.close();
			} catch (IOException e) {}
		}
	}
}
