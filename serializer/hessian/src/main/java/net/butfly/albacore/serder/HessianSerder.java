package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.http.entity.ContentType;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Hessian2StreamingInput;
import com.caucho.hessian.io.Hessian2StreamingOutput;
import com.caucho.hessian.io.SerializerFactory;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.serder.support.SerderFactorySupport;
import net.butfly.albacore.utils.Reflections;

public class HessianSerder extends TextSerderBase<Object> implements ArrableTextSerder<Object>, SerderFactorySupport,
		ClassInfoSerder<Object, CharSequence>, BeanSerder<CharSequence> {
	private static final long serialVersionUID = -593535528324149595L;

	public HessianSerder() {
		super(ContentTypes.APPLICATION_HESSIAN);
	}

	public HessianSerder(ContentType... contentType) {
		super(ContentTypes.APPLICATION_HESSIAN, contentType);
	}

	@Override
	public <T> String ser(T src) {
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> T der(CharSequence dst, Class<T> to) {
		ByteArrayInputStream in = new ByteArrayInputStream(dst.toString().getBytes(contentType().getCharset()));
		Hessian2StreamingInput hi = new Hessian2StreamingInput(in);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (T) hi.readObject();
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
	public Object[] der(CharSequence from, Class<?>... to) {
		ByteArrayInputStream in = new ByteArrayInputStream(from.toString().getBytes(contentType().getCharset()));
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
