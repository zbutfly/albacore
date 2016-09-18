package net.butfly.bus.serialize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import net.butfly.albacore.utils.Utils;
import scala.Tuple2;

public final class Jsons extends Utils {
	public static ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS).enable(
			SerializationFeature.WRITE_ENUMS_USING_INDEX).enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL).enable(
					SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

	public static <T> T parseNode(JsonNode node, Class<T> to) {
		try {
			return mapper.treeToValue(node, to);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static JsonNode[] dearray(CharSequence src) throws JsonProcessingException, IOException {
		final JsonNode node = Jsons.mapper.readTree(src.toString());
		if (node.isNull()) return null;
		if (!node.isArray()) return new JsonNode[] { node };
		List<JsonNode> nodes = new ArrayList<>();
		Iterator<JsonNode> it = node.iterator();
		while (it.hasNext())
			nodes.add(it.next());
		return nodes.toArray(new JsonNode[nodes.size()]);
	}

	private static final JsonSerder jsd = new JsonSerder();

	@SafeVarargs
	@SuppressWarnings("unchecked")
	public static String simpleJSON(String key, Object value, Tuple2<String, Object>... kvs) {
		Map<String, Object> map = new HashMap<>();
		map.put(key, value);
		for (Tuple2<String, ?> e : kvs)
			map.put(e._1, e._2);
		return jsd.serialize(map);
	}

	public static String simpleJSON(Map<String, ?> map) {
		return jsd.serialize(map);
	}
}
