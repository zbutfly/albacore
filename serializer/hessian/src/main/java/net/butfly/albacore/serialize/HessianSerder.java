package net.butfly.albacore.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.http.entity.ContentType;

import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.Hessian2StreamingInput;
import com.caucho.hessian.io.Hessian2StreamingOutput;
import com.caucho.hessian.io.SerializerFactory;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.TextSerder;
import net.butfly.albacore.serder.TextSerderBase;
import net.butfly.albacore.serder.support.ClassInfo;
import net.butfly.albacore.serder.support.ClassInfo.ClassInfoSupport;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.serder.support.SerderFactorySupport;
import net.butfly.albacore.utils.Reflections;

@SuppressWarnings("rawtypes")
@ClassInfo(ClassInfoSupport.RESTRICT)
public class HessianSerder<PRESENT> extends TextSerderBase<PRESENT> implements TextSerder<PRESENT>, SerderFactorySupport {
	private static final long serialVersionUID = -593535528324149595L;

	public HessianSerder() {
		super(ContentTypes.APPLICATION_HESSIAN);
	}

	public HessianSerder(ContentType contentType) {
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
	public Object deserialize(CharSequence dst, Class to) {
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
	public Object[] deserialize(CharSequence dst, Class<?>[] types) {
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
