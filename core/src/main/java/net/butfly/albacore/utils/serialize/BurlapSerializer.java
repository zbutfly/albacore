package net.butfly.albacore.utils.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.caucho.burlap.io.BurlapInput;
import com.caucho.burlap.io.BurlapOutput;
import com.caucho.hessian.io.AbstractSerializerFactory;
import com.caucho.hessian.io.SerializerFactory;

public class BurlapSerializer extends HTTPStreamingSupport implements Serializer, SerializerFactorySupport {
	private SerializerFactory factory;

	@Override
	public void write(OutputStream os, Object obj) throws IOException {
		BurlapOutput ho = new BurlapOutput(os);
		if (null != factory) ho.setSerializerFactory(factory);
		try {
			ho.writeObject(obj);
		} finally {
			ho.close();
		}
		os.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(InputStream is, Class<?>... types) throws IOException {
		BurlapInput hi = new BurlapInput(is);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (T) hi.readObject();
		} finally {
			hi.close();
		}
	}

	@Override
	public void readThenWrite(InputStream is, OutputStream os, Class<?>... types) throws IOException {
		write(os, read(is, types));
	}

	@Override
	public boolean supportHTTPStream() {
		return true;
	}

	@Override
	public String[] getContentTypes() {
		return new String[] { "x-application/burlap" };
	}

	@Override
	public void addFactory(AbstractSerializerFactory factory) {
		if (this.factory == null) this.factory = new SerializerFactory();
		this.factory.addFactory(factory);
	}
}
