package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albacore.serder.support.ByteArray;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.utils.CaseFormat;

public class BsonSerder extends BinarySerderBase<Object> implements ArrableBinarySerder<Object>, BeanSerder<ByteArray> {
	private static final long serialVersionUID = 6664350391207228363L;

	public BsonSerder() {
		super(ContentTypes.APPLICATION_BSON);
	}

	public BsonSerder(ContentType contentType) {
		super(ContentTypes.APPLICATION_BSON, contentType);
	}

	@Override
	public <T> ByteArray ser(T from) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			Jsons.bsoner.writeValue(baos, from);
			return new ByteArray(baos);
		} catch (IOException e) {
			return null;
		} finally {
			try {
				baos.close();
			} catch (IOException e) {}
		}
	}

	@SafeVarargs
	@Override
	public final Object[] der(ByteArray from, TypeToken<? extends Object>... tos) {
		try {
			JsonNode[] n = Jsons.array(Jsons.bsoner.readTree(from.get()));
			if (n == null) return null;
			Object[] r = new Object[Math.min(tos.length, n.length)];
			for (int i = 0; i < r.length; i++)
				r[i] = Jsons.bsoner.readValue(Jsons.bsoner.treeAsTokens(n[i]), Jsons.bsoner.constructType(tos[i].getType()));
			return r;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T> T der(ByteArray from, TypeToken<T> to) {
		if (null == from) return null;
		ByteArrayInputStream bais = from.input();
		try {
			return Jsons.bsoner.readValue(bais, Jsons.mapper.constructType(to.getType()));
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
		to.write(ser(from).get());
	}

	@Override
	public <T> T der(InputStream in, TypeToken<T> to) throws IOException {
		return der(new ByteArray(in), to);
	}

	private CaseFormat format = CaseFormat.NO_CHANGE;

	@Override
	public BsonSerder mapping(CaseFormat to) {
		this.format = to;
		return this;
	}

	@Override
	public CaseFormat mapping() {
		return format;
	}
}
