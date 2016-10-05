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
import com.google.common.reflect.TypeToken;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.support.ByteArray;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.serder.support.SerderFactorySupport;
import net.butfly.albacore.utils.Reflections;

public class BurlapSerder extends BinarySerderBase<Object> implements ArrableBinarySerder<Object>, SerderFactorySupport,
		ClassInfoSerder<Object, ByteArray> {
	private static final long serialVersionUID = 691937271877170782L;

	public BurlapSerder() {
		super(ContentTypes.APPLICATION_BURLAP);
	}

	public BurlapSerder(ContentType... contentType) {
		super(ContentTypes.APPLICATION_BURLAP, contentType);
	}

	@Override
	public <T> ByteArray ser(T from) {

		try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {
			ser(from, out);
			return new ByteArray(out);
		} catch (IOException ex) {
			return null;
		}
	}

	@Override
	public <T> T der(ByteArray from, TypeToken<T> to) {
		try (ByteArrayInputStream in = from.input();) {
			return der(in, to);
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
	public <T> T der(InputStream in, TypeToken<T> to) throws IOException {
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
	public final Object[] der(ByteArray from, Class<?>... tos) {
		try (ByteArrayInputStream in = from.input();) {
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
