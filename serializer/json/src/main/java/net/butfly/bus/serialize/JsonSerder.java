package net.butfly.bus.serialize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.ArrableSerder;
import net.butfly.albacore.serder.ContentSerderBase;
import net.butfly.albacore.serder.TextSerder;

@SuppressWarnings("rawtypes")
public class JsonSerder extends ContentSerderBase<CharSequence> implements TextSerder, ArrableSerder<CharSequence> {
	private static final long serialVersionUID = -4394900785541475884L;
	private static ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS).enable(
			SerializationFeature.WRITE_ENUMS_USING_INDEX).enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL).enable(
					SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

	public JsonSerder() {
		super(ContentType.APPLICATION_JSON);
	}

	public JsonSerder(ContentType contentType) {
		super(contentType);
	}

	@Override
	public String serialize(Object obj) {
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new SystemException("", e);
		}
	}

	@SuppressWarnings("unchecked")
	// XXX
	@Override
	public Object deserialize(CharSequence src, Class type) {
		try {
			return mapper.readValue(src.toString(), type);
		} catch (IOException e) {
			throw new IllegalArgumentException("Invalid JSON.");
		}
	}

	@Override
	public Object[] deserialize(CharSequence src, Class[] types) {
		String data = src.toString();
		try {
			final JsonNode node = mapper.readTree(data);
			if (node.isNull()) return null;
			// 1 argument required, dynamic array wrapping.
			if (types.length == 1) {
				Object r;
				// array wrapped parameters
				if (node.isArray()) r = node.size() == 0 ? null : mapper.treeToValue(node.get(0), (Class<?>) types[0]);
				// 1 parameter only
				else r = mapper.treeToValue(node, (Class<?>) types[0]);
				return new Object[] { r };
			} else {
				if (!node.isArray()) throw new IllegalArgumentException("Need array for multiple arguments.");
				Iterator<JsonNode> it = node.iterator();
				Object[] args = new Object[types.length];
				for (int i = 0; i < types.length; i++) {
					if (!it.hasNext()) throw new IllegalArgumentException("Less amount arguments recieved than required.");
					args[i] = mapper.treeToValue(it.next(), (Class<?>) types[i]);
				}
				return args;
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Invalid JSON.");
		}
	}
}
