package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.caucho.burlap.io.BurlapInput;
import com.caucho.burlap.io.BurlapOutput;
import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.SerializerFactory;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.support.BinarySerder;
import net.butfly.albacore.serder.support.ClassInfoSerder;
import net.butfly.albacore.serder.support.ContentTypeSerderBase;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.serder.support.SerderFactorySupport;
import net.butfly.albacore.utils.Reflections;

public final class BurlapSerder extends ContentTypeSerderBase implements BinarySerder<Object>, SerderFactorySupport,
		ClassInfoSerder<Object, byte[]> {
	private static final long serialVersionUID = 691937271877170782L;

	public BurlapSerder() {
		this.contentType = ContentTypes.APPLICATION_BURLAP;
	}

	@Override
	public byte[] ser(Object from) {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {
			ser(from, out);
			return out.toByteArray();
		} catch (IOException ex) {
			return null;
		}
	}

	@Override
	public <T> T der(byte[] from) {
		return der(from, (Class<T>) null);
	}

	@Override
	public <T> T der(byte[] from, Class<T> to) {
		try (ByteArrayInputStream in = new ByteArrayInputStream(from);) {
			return der(in, null);
		} catch (IOException ex) {
			return null;
		}
	}

	@Override
	public void ser(Object from, OutputStream to) throws IOException {
		BurlapOutput ho = new BurlapOutput(to);
		if (null != factory) ho.setSerializerFactory(factory);
		try {
			ho.writeObject(from);
		} finally {
			ho.close();
		}
		to.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T der(InputStream in, Class<T> clazz) throws IOException {
		BurlapInput hi = new BurlapInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (T) hi.readObject();
		} finally {
			hi.close();
		}
	}

	@Override
	@SafeVarargs
	public final Object[] der(byte[] from, Class<?>... tos) {
		try (ByteArrayInputStream in = new ByteArrayInputStream(from);) {
			return (Object[]) der(in, null);
		} catch (IOException e) {
			return null;
		}
	}

	protected SerializerFactory factory;

	@Override
	public void addFactories(String... classes) {
		if (this.factory == null) this.factory = new SerializerFactory();
		if (null != classes) for (String f : classes)
			try {
				AbstractSerializerFactory fact = Reflections.construct(f);
				this.factory.addFactory(fact);
			} catch (Exception e) {
				throw new SystemException("", e);
			}
	}
}
