package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.databind.JsonNode;

import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albacore.serder.support.ContentTypes;

public class BsonSerder extends BinarySerderBase<Object> implements ArrableBinarySerder<Object>, BeanSerder<byte[]> {
	private static final long serialVersionUID = 6664350391207228363L;

	public BsonSerder() {
		super(ContentTypes.APPLICATION_BSON);
	}

	public BsonSerder(ContentType contentType) {
		super(ContentTypes.APPLICATION_BSON, contentType);
	}

	@Override
	public <T> byte[] ser(T from) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			Jsons.bsoner.writeValue(baos, from);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		} finally {
			try {
				baos.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public Object[] der(byte[] from, Class<?>... tos) {
		try {
			JsonNode[] n = Jsons.deArray(Jsons.bsoner.readTree(from));
			if (n == null) return null;
			Object[] r = new Object[Math.min(tos.length, n.length)];
			for (int i = 0; i < r.length; i++)
				r[i] = Jsons.bsoner.treeToValue(n[i], tos[i]);
			return r;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T> T der(byte[] from, Class<T> to) {
		if (null == from) return null;
		ByteArrayInputStream bais = new ByteArrayInputStream(from);
		try {
			return Jsons.bsoner.readValue(bais, to);
		} catch (IOException e) {
			return null;
		} finally {
			try {
				bais.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public void ser(Object from, OutputStream to) throws IOException {
		to.write(ser(from));
	}

	@Override
	public <T> T der(InputStream in, Class<T> to) throws IOException {
		return der(IOUtils.toByteArray(in), to);
	}
}
