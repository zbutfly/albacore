//package net.butfly.albacore.serder.support;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.Serializable;
//
//import net.butfly.albacore.utils.Reflections;
//
//public final class ByteArray implements Serializable {
//	private static final long serialVersionUID = -1384990797210247805L;
//	public static final Class<byte[]> TYPE = byte[].class;
//	private final byte[] data;
//
//	public ByteArray(final byte[] data) {
//		super();
//		Reflections.noneNull("null byte array not allow", data);
//		this.data = data;
//	}
//
//	public ByteArray(final InputStream input) {
//		this(readAll(input));
//	}
//
//	public ByteArray(final ByteArrayOutputStream output) {
//		this(output.toByteArray());
//	}
//
//	private static byte[] readAll(final InputStream is) {
//		Reflections.noneNull("null byte array not allow", is);
//		try (ByteArrayOutputStream os = new ByteArrayOutputStream();) {
//			byte[] buffer = new byte[1024];
//			int n;
//			while (-1 != (n = is.read(buffer)))
//				os.write(buffer, 0, n);
//			return os.toByteArray();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	public byte[] get() {
//		return data;
//	}
//
//	public ByteArrayOutputStream output() {
//		ByteArrayOutputStream os = new ByteArrayOutputStream();
//		try {
//			os.write(data);
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//		return os;
//	}
//
//	public ByteArrayInputStream input() {
//		return new ByteArrayInputStream(data);
//	}
//}
