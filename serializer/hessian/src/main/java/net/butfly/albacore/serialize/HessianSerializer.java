package net.butfly.albacore.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.http.entity.ContentType;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Hessian2StreamingInput;
import com.caucho.hessian.io.Hessian2StreamingOutput;
import com.caucho.hessian.io.SerializerFactory;
import com.google.common.base.Charsets;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serializer.SerializerBase;
import net.butfly.albacore.serializer.SerializerFactorySupport;
import net.butfly.albacore.serializer.TextSerializer;
import net.butfly.albacore.utils.Reflections;

public class HessianSerializer extends SerializerBase<CharSequence> implements TextSerializer, SerializerFactorySupport {
	public HessianSerializer() {
		super(ContentType.create("x-application/hessian", Charsets.UTF_8));
	}

	public HessianSerializer(ContentType contentType) {
		super(contentType);
	}

	@Override
	public CharSequence serialize(Serializable src) {
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
	public Serializable deserialize(CharSequence dst, Class<? extends Serializable> srcClass) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst.toString().getBytes(contentType().getCharset()));
		Hessian2StreamingInput hi = new Hessian2StreamingInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (Serializable) hi.readObject();
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
	public Serializable[] deserialize(CharSequence dst, Class<? extends Serializable>[] types) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst.toString().getBytes(contentType().getCharset()));
		Hessian2StreamingInput hi = new Hessian2StreamingInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		Object[] r;
		try {
			r = (Object[]) hi.readObject();
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
		Serializable[] rr = new Serializable[r.length];
		for (int i = 0; i < r.length; i++)
			rr[i] = (Serializable) r[i];
		return rr;
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
