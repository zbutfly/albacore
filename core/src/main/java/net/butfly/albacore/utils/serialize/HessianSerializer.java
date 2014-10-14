package net.butfly.albacore.utils.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.io.output.WriterOutputStream;

import com.caucho.burlap.io.BurlapInput;
import com.caucho.burlap.io.BurlapOutput;
import com.caucho.hessian.io.Hessian2StreamingInput;
import com.caucho.hessian.io.Hessian2StreamingOutput;
import com.caucho.hessian.io.SerializerFactory;

public class HessianSerializer implements Serializer {
	private SerializerFactory factory;

	@Override
	public void write(Writer writer, Object obj) throws IOException {
		BurlapOutput ho = new BurlapOutput(new WriterOutputStream(writer));
		if (null != factory) ho.setSerializerFactory(factory);
		try {
			ho.writeObject(obj);
		} finally {
			ho.close();
		}
		writer.flush();
	}

	@Override
	public void write(OutputStream os, Object obj) throws IOException {
		Hessian2StreamingOutput ho = new Hessian2StreamingOutput(os);
		if (null != factory) ho.getHessian2Output().setSerializerFactory(factory);
		ho.setCloseStreamOnClose(false);
		try {
			ho.writeObject(obj);
		} finally {
			ho.close();
		}
		os.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(Reader reader, Class<?>... types) throws IOException {
		BurlapInput hi = new BurlapInput(new ReaderInputStream(reader));
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (T) hi.readObject();
		} finally {
			hi.close();
		}
	}

	@Override
	public void read(Reader reader, Writer writer, Class<?>... types) throws IOException {
		BurlapInput hi = new BurlapInput(new ReaderInputStream(reader));
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			hi.readToOutputStream(new WriterOutputStream(writer));
		} finally {
			hi.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(InputStream is, Class<?>... types) throws IOException {
		Hessian2StreamingInput hi = new Hessian2StreamingInput(is);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			return (T) hi.readObject();
		} finally {
			hi.close();
		}
	}

	@Override
	public void read(InputStream is, OutputStream os, Class<?>... types) throws IOException {
		Hessian2StreamingInput hi = new Hessian2StreamingInput(is);
		if (null != factory) hi.setSerializerFactory(factory);
		try {
			hi.startPacket().readToOutputStream(os);
			hi.endPacket();
		} finally {
			hi.close();
		}
	}

	public void setFactory(SerializerFactory factory) {
		this.factory = factory;
	}

	@Override
	public boolean supportHTTPStream() {
		return true;
	}
}
