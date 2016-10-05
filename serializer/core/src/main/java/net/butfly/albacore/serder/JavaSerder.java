package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.http.entity.ContentType;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.support.ByteArray;
import net.butfly.albacore.serder.support.ClassInfo;
import net.butfly.albacore.utils.Reflections;

@ClassInfo
public class JavaSerder extends BinarySerderBase<Object> implements ArrableBinarySerder<Object>, ClassInfoSerder<Object, ByteArray> {
	private static final long serialVersionUID = 2446148201514088203L;

	public JavaSerder() {
		super();
	}

	public JavaSerder(ContentType... contentType) {
		super(contentType);
	}

	@Override
	@SafeVarargs
	public final Object[] der(ByteArray from, Class<?>... tos) {
		try {
			return (Object[]) der(from.input(), null);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T> ByteArray ser(T from) {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream();) {
			ser(from, os);
			return new ByteArray(os);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T> T der(ByteArray from, TypeToken<T> to) {
		try {
			return der(from.input(), to);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T> void ser(T from, OutputStream to) throws IOException {
		if (from == null) return;
		Class<?> cl = from.getClass();
		if (byte[].class.isAssignableFrom(cl)) to.write((byte[]) from);
		else if (ByteArray.class.isAssignableFrom(cl)) to.write(((ByteArray) from).get());
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
	public <T> T der(InputStream from, TypeToken<T> to) throws IOException {
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
