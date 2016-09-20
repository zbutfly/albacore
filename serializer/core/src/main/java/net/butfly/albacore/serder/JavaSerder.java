package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.http.entity.ContentType;

import net.butfly.albacore.serder.support.ClassInfo;
import net.butfly.albacore.utils.Reflections;

@ClassInfo
public class JavaSerder extends BinarySerderBase<Object> implements ArrableBinarySerder<Object>, ClassInfoSerder<Object, byte[]>,
		BeanSerder<byte[]> {
	private static final long serialVersionUID = 2446148201514088203L;

	public JavaSerder() {
		super();
	}

	public JavaSerder(ContentType... contentType) {
		super(contentType);
	}

	@Override
	@SafeVarargs
	public final Object[] der(byte[] from, Class<? extends Object>... tos) {
		try {
			return (Object[]) der(new ByteArrayInputStream(from), null);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T> byte[] ser(T from) {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			ser(from, os);
		} catch (IOException e) {
			return null;
		}
		return os.toByteArray();
	}

	@Override
	public <T> T der(byte[] from, Class<T> to) {
		try {
			return der(new ByteArrayInputStream(from), to);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public void ser(Object from, OutputStream to) throws IOException {
		ObjectOutputStream oos = Reflections.wrap(to, ObjectOutputStream.class);
		try {
			oos.writeObject(from);
			oos.flush();
		} finally {
			oos.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T der(InputStream from, Class<T> to) throws IOException {
		ObjectInputStream ois = Reflections.wrap(from, ObjectInputStream.class);
		try {
			return (T) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		} finally {
			ois.close();
		}
	}

}
