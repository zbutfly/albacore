package net.butfly.bus.serialize;

import java.io.IOException;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.TextSerder;
import net.butfly.albacore.serder.TextSerderBase;
import net.butfly.albacore.serder.support.ClassInfo;

@SuppressWarnings("rawtypes")
@ClassInfo
public class JsonSerder extends TextSerderBase<Object> implements TextSerder<Object> {
	private static final long serialVersionUID = -4394900785541475884L;

	public JsonSerder() {
		super(ContentType.APPLICATION_JSON);
	}

	public JsonSerder(ContentType contentType) {
		super(contentType);
	}

	@Override
	public String serialize(Object obj) {
		try {
			return Jsons.mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new SystemException("", e);
		}
	}

	@SuppressWarnings("unchecked")
	// XXX
	@Override
	public Object deserialize(CharSequence src, Class type) {
		try {
			return Jsons.mapper.readValue(src.toString(), type);
		} catch (IOException e) {
			throw new IllegalArgumentException("Invalid JSON.");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object[] deserialize(CharSequence from, Class[] tos) {
		try {
			JsonNode[] n = Jsons.dearray(from);
			if (n == null) return null;
			Object[] r = new Object[Math.min(tos.length, n.length)];
			for (int i = 0; i < r.length; i++)
				r[i] = Jsons.mapper.treeToValue(n[i], tos[i]);
			return r;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
