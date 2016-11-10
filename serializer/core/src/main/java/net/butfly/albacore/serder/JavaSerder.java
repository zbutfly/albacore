package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;

import net.butfly.albacore.serder.support.BinarySerder;
import net.butfly.albacore.serder.support.ClassInfoSerder;
import net.butfly.albacore.utils.Reflections;

public class JavaSerder implements BinarySerder<Object>, ClassInfoSerder<Object, byte[]> {
	private static final long serialVersionUID = 2446148201514088203L;

	public JavaSerder() {
		super();
	}

	@Override
	@SafeVarargs
	public final Object[] der(byte[] from, Class<?>... tos) {
		return (Object[]) der(from);
	}

	@Override
	public byte[] ser(Object from) {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream();) {
			ser(from, os);
			return os.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T> T der(byte[] from) {
		try (InputStream in = new ByteArrayInputStream(from);) {
			return der(in);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public void ser(Object from, OutputStream to) throws IOException {
		to.write(ser(from));
	}

	@SuppressWarnings("unchecked")
	public <T> T der(InputStream from) throws IOException {
		try (ObjectInputStream ois = Reflections.wrap(from, ObjectInputStream.class);) {
			return (T) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	@Override
	public <T> T der(byte[] from, Class<T> to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T der(InputStream from, Class<T> to) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
