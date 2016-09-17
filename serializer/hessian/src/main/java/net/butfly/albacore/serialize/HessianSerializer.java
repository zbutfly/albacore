package net.butfly.albacore.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.http.entity.ContentType;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Hessian2StreamingInput;
import com.caucho.hessian.io.Hessian2StreamingOutput;
import com.caucho.hessian.io.SerializerFactory;
import com.google.common.base.Charsets;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serializer.ArraySerializer;
import net.butfly.albacore.serializer.ContentSerializerBase;
import net.butfly.albacore.serializer.SerializerFactorySupport;
import net.butfly.albacore.serializer.TextSerializer;
import net.butfly.albacore.utils.Reflections;

@SuppressWarnings("rawtypes")
public class HessianSerializer extends ContentSerializerBase<CharSequence> implements TextSerializer, ArraySerializer<CharSequence>,
		SerializerFactorySupport {
	private static final long serialVersionUID = -593535528324149595L;

	public HessianSerializer() {
		super(ContentType.create("x-application/hessian", Charsets.UTF_8));
	}

	public HessianSerializer(ContentType contentType) {
		super(contentType);
	}

	@Override
	public CharSequence serialize(Object src) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Hessian2StreamingOutput ho = new Hessian2StreamingOutput(out);
		if (null != factory) ho.getHessian2Output().setSerializerFactory(factory);
		ho.setCloseStreamOnClose(false);
		try {
			ho.writeObject(src);
			ho.flush();
		} catch (IOException e) {
			throw new SystemException("", e);
		}

		try {
			return new String(out.toByteArray(), contentType().getCharset());
		} finally {
			try {
				ho.close();
			} catch (IOException e) {}
			try {
				out.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public Object deserialize(CharSequence dst, Class srcClass) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst.toString().getBytes(contentType().getCharset()));
		Hessian2StreamingInput hi = new Hessian2StreamingInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (Object) hi.readObject();
		} catch (IOException e) {
			throw new SystemException("", e);
		} finally {
			try {
				hi.close();
			} catch (IOException e) {}
			try {
				in.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public Object[] deserialize(CharSequence dst, Class[] types) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst.toString().getBytes(contentType().getCharset()));
		Hessian2StreamingInput hi = new Hessian2StreamingInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (Object[]) hi.readObject();
		} catch (IOException e) {
			throw new SystemException("", e);
		} finally {
			try {
				hi.close();
			} catch (IOException e) {}
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
