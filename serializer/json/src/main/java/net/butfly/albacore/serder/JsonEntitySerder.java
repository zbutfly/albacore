package net.butfly.albacore.serder;

import java.io.IOException;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.reflect.TypeToken;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albacore.utils.CaseFormat;

public class JsonEntitySerder<E> extends TextSerderBase<E> implements ArrableTextSerder<E>, BeanSerder<E, CharSequence> {
	private static final long serialVersionUID = -4394900785541475884L;

	public JsonEntitySerder() {
		super(ContentType.APPLICATION_JSON);
	}

	public JsonEntitySerder(ContentType... contentType) {
		super(contentType);
		enable(ContentType.APPLICATION_JSON);
	}

	@Override
	public <T extends E> String ser(T from) {
		try {
			return Jsons.mapper.writeValueAsString(from);
		} catch (JsonProcessingException e) {
			throw new SystemException("", e);
		}
	}

	@Override
	@SafeVarargs
	public final Object[] der(CharSequence from, Class<?>... tos) {
		try {
			JsonNode[] n = Jsons.array(Jsons.mapper.readTree(from.toString()));
			if (n == null) return null;
			Object[] r = new Object[Math.min(tos.length, n.length)];
			for (int i = 0; i < r.length; i++)
				r[i] = Jsons.mapper.readValue(Jsons.mapper.treeAsTokens(n[i]), tos[i]);
			return r;
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T extends E> T der(CharSequence from, TypeToken<T> to) {
		try {
			return Jsons.mapper.readValue(from.toString(), Jsons.mapper.constructType(to.getType()));
		} catch (IOException e) {
			return null;
		}
	}

	private CaseFormat format = CaseFormat.ORIGINAL;

	@Override
	public JsonEntitySerder<E> mapping(CaseFormat to) {
		this.format = to;
		return this;
	}

	@Override
	public CaseFormat mapping() {
		return format;
	}
}
