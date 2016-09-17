package net.butfly.bus.serialize;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serializer.SerializerBase;
import net.butfly.albacore.serializer.TextSerializer;

public class JSONSerializer extends SerializerBase<CharSequence> implements TextSerializer {
	private static ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS).enable(
			SerializationFeature.WRITE_ENUMS_USING_INDEX).enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL).enable(
					SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

	public JSONSerializer() {
		super(ContentType.APPLICATION_JSON);
	}

	public JSONSerializer(ContentType contentType) {
		super(contentType);
	}

	@Override
	public String serialize(Serializable obj) {
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new SystemException("", e);
		}
	}

	@Override
	public Serializable deserialize(CharSequence src, Class<? extends Serializable> type) {
		try {
			return mapper.readValue(src.toString(), type);
		} catch (IOException e) {
			throw new IllegalArgumentException("Invalid JSON.");
		}
	}

	@Override
	public Serializable[] deserialize(CharSequence src, Class<? extends Serializable>[] types) {
		String data = src.toString();
		try {
			final JsonNode node = mapper.readTree(data);
			if (node.isNull()) return null;
			// 1 argument required, dynamic array wrapping.
			if (types.length == 1) {
				Serializable r;
				// array wrapped parameters
				if (node.isArray()) r = node.size() == 0 ? null : (Serializable) mapper.treeToValue(node.get(0), (Class<?>) types[0]);
				// 1 parameter only
				else r = (Serializable) mapper.treeToValue(node, (Class<?>) types[0]);
				return new Serializable[] { r };
			} else {
				if (!node.isArray()) throw new IllegalArgumentException("Need array for multiple arguments.");
				Iterator<JsonNode> it = node.iterator();
				Serializable[] args = new Serializable[types.length];
				for (int i = 0; i < types.length; i++) {
					if (!it.hasNext()) throw new IllegalArgumentException("Less amount arguments recieved than required.");
					args[i] = (Serializable) mapper.treeToValue(it.next(), (Class<?>) types[i]);
				}
				return args;
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Invalid JSON.");
		}
	}
}
