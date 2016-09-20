package net.butfly.albacore.serder;

import java.io.IOException;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.json.Jsons;

public class JsonSerder extends TextSerderBase<Object> implements ArrableTextSerder<Object>, BeanSerder<CharSequence> {
	private static final long serialVersionUID = -4394900785541475884L;

	public JsonSerder() {
		super(ContentType.APPLICATION_JSON);
	}

	public JsonSerder(ContentType... contentType) {
		super(contentType);
		enable(ContentType.APPLICATION_JSON);
	}

	@Override
	public <T> String ser(T from) {
		try {
			return Jsons.mapper.writeValueAsString(from);
		} catch (JsonProcessingException e) {
			throw new SystemException("", e);
		}
	}

	@Override
	public Object[] der(CharSequence from, Class<?>... tos) {
		try {
			JsonNode[] n = Jsons.deArray(Jsons.mapper.readTree(from.toString()));
			if (n == null) return null;
			Object[] r = new Object[Math.min(tos.length, n.length)];
			for (int i = 0; i < r.length; i++)
				r[i] = Jsons.mapper.treeToValue(n[i], tos[i]);
			return r;
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T> T der(CharSequence from, Class<T> to) {
		try {
			return Jsons.mapper.readValue(from.toString(), to);
		} catch (IOException e) {
			return null;
		}
	}
}
