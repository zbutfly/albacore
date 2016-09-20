package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.http.entity.ContentType;

import com.caucho.burlap.io.BurlapInput;
import com.caucho.burlap.io.BurlapOutput;
import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.SerializerFactory;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.serder.support.SerderFactorySupport;
import net.butfly.albacore.utils.Reflections;

public class BurlapSerder extends BinarySerderBase<Object> implements ArrableBinarySerder<Object>, SerderFactorySupport,
		ClassInfoSerder<Object, byte[]>, BeanSerder<byte[]> {
	private static final long serialVersionUID = 691937271877170782L;

	public BurlapSerder() {
		super(ContentTypes.APPLICATION_BURLAP);
	}

	public BurlapSerder(ContentType... contentType) {
		super(ContentTypes.APPLICATION_BURLAP, contentType);
	}

	@Override
	public <T> byte[] ser(T from) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			ser(from, out);
		} catch (IOException ex) {
			throw new SystemException("", ex);
		}
		try {
			return out.toByteArray();
		} finally {
			try {
				out.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public <T> T der(byte[] dst, Class<T> to) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst);
		try {
			return der(in, to);
		} catch (IOException ex) {
			throw new SystemException("", ex);
		} finally {
			try {
				in.close();
			} catch (IOException e) {}
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
	public <T> T der(InputStream in, Class<T> to) throws IOException {
		BurlapInput hi = new BurlapInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (T) hi.readObject();
		} finally {
			hi.close();
		}
	}

	@Override
	public Object[] der(byte[] from, Class<?>... tos) {
		ByteArrayInputStream in = new ByteArrayInputStream(from);
		try {
			return (Object[]) der(in, null);
		} catch (IOException e) {
			throw new SystemException("", e);
		} finally {
			try {
				in.close();
			} catch (IOException e) {}
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
