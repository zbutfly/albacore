package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albacore.serder.support.ByteArray;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.utils.CaseFormat;

public class BsonMapSerder extends BinarySerderBase<Map<String, Object>> implements ArrableBinarySerder<Map<String, Object>>,
		MappingSerder<Map<String, Object>, ByteArray> {
	private static final long serialVersionUID = 6664350391207228363L;
	private static final JavaType t = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class);

	public BsonMapSerder() {
		super(ContentTypes.APPLICATION_BSON);

	}

	public BsonMapSerder(ContentType contentType) {
		super(ContentTypes.APPLICATION_BSON, contentType);
	}

	@Override
	public <T extends Map<String, Object>> ByteArray ser(T from) {
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

	@SuppressWarnings("unchecked")
	@SafeVarargs
	@Override
	public final Map<String, Object>[] der(ByteArray from, Class<?>... tos) {
		try {
			JsonNode[] n = Jsons.array(Jsons.bsoner.readTree(from.get()));
			if (n == null) return null;
			List<Map<String, Object>> r = new ArrayList<>();
			for (int i = 0; i < n.length; i++)
				r.add(Jsons.bsoner.readValue(Jsons.bsoner.treeAsTokens(n[i]), t));
			return r.toArray(new Map[0]);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends Map<String, Object>> T der(ByteArray from, TypeToken<T> to) {
		if (null == from) return null;
		ByteArrayInputStream bais = from.input();
		try {
			return Jsons.bsoner.readValue(bais, t);
		} catch (IOException e) {
			return null;
		} finally {
			try {
				bais.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public <T extends Map<String, Object>> void ser(T from, OutputStream to) throws IOException {
		to.write(ser(from).get());
	}

	@Override
	public <T extends Map<String, Object>> T der(InputStream in, TypeToken<T> to) throws IOException {
		return der(new ByteArray(in), to);
	}

	private CaseFormat format = CaseFormat.ORIGINAL;

	@Override
	public BsonMapSerder mapping(CaseFormat to) {
		this.format = to;
		return this;
	}

	@Override
	public CaseFormat mapping() {
		return format;
	}
}
