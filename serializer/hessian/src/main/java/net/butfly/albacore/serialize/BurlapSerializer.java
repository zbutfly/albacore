package net.butfly.albacore.serialize;

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
import com.google.common.base.Charsets;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serializer.ArraySerializer;
import net.butfly.albacore.serializer.BinarySerializer;
import net.butfly.albacore.serializer.ContentSerializerBase;
import net.butfly.albacore.serializer.SerializerFactorySupport;
import net.butfly.albacore.utils.Reflections;

@SuppressWarnings("rawtypes")
public class BurlapSerializer extends ContentSerializerBase<byte[]> implements BinarySerializer, ArraySerializer<byte[]>,
		SerializerFactorySupport {
	private static final long serialVersionUID = 691937271877170782L;

	public BurlapSerializer() {
		super(ContentType.create("x-application/burlap", Charsets.UTF_8));
	}

	public BurlapSerializer(ContentType contentType) {
		super(contentType);
	}

	@Override
	public byte[] serialize(Object src) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			this.serialize(out, src);
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
	public Object deserialize(byte[] dst, Class srcClass) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst);
		try {
			return deserialize(in, srcClass);
		} catch (IOException ex) {
			throw new SystemException("", ex);
		} finally {
			try {
				in.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public void serialize(OutputStream out, Object src) throws IOException {
		BurlapOutput ho = new BurlapOutput(out);
		if (null != factory) ho.setSerializerFactory(factory);
		try {
			ho.writeObject(src);
		} finally {
			ho.close();
		}
		out.flush();
	}

	@Override
	public Object deserialize(InputStream in, Class srcClass) throws IOException {
		BurlapInput hi = new BurlapInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return hi.readObject();
		} finally {
			hi.close();
		}
	}

	@Override
	public Object[] deserialize(byte[] dst, Class[] types) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst);
		try {
			return (Object[]) deserialize(in, null);
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
