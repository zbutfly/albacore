package net.butfly.albacore.serder.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonParser;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;
import scala.Tuple2;

public final class Jsons extends Utils {
	private static final JsonFactory DEFAULT_BSON_FACTORY = new BsonFactory()//
			// .enable(BsonGenerator.Feature.ENABLE_STREAMING)//cause EOF
			.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH);
	public static ObjectMapper mapper = defaultJsonMapper();
	public static ObjectMapper bsoner = defaultBsonMapper();//

	public static <T> T parse(JsonNode node, Class<T> to) {
		try {
			return mapper.treeToValue(node, to);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	private static ObjectMapper defaultBsonMapper() {
		return new ObjectMapper(DEFAULT_BSON_FACTORY) //
				// .setPropertyNamingStrategy(new
				// UpperCaseWithUnderscoresStrategy())//
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)//
				.disable(MapperFeature.USE_GETTERS_AS_SETTERS)//
				.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)//
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)//
				.setSerializationInclusion(Include.NON_NULL)//
				.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)//
				.configure(JsonParser.Feature.IGNORE_UNDEFINED, true)//
		;
	}

	private static ObjectMapper defaultJsonMapper() {
		return new ObjectMapper()//
				.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)//
				.configure(JsonParser.Feature.IGNORE_UNDEFINED, true)//
				.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)//
				.enable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)//
				.enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)//
				.enable(SerializationFeature.WRITE_ENUMS_USING_INDEX)//
				.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)//
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)//
		;
	}

	public static JsonNode[] array(JsonNode node) throws JsonProcessingException, IOException {
		if (node.isNull()) return null;
		if (node.isArray()) {
			List<JsonNode> nodes = new ArrayList<>();
			Iterator<JsonNode> it = node.iterator();
			while (it.hasNext())
				nodes.add(it.next());
			return nodes.toArray(new JsonNode[nodes.size()]);
		}
		int len = 0;
		for (;; len++)
			if (!node.has(Integer.toString(len))) break;
		if (len > 0) {
			List<JsonNode> nodes = new ArrayList<>();
			for (int i = 0; i < len; i++)
				nodes.add(node.get(Integer.toString(i)));
			return nodes.toArray(new JsonNode[nodes.size()]);
		}
		return new JsonNode[] { node };
	}

	public static int arraySize(JsonNode node) {
		if (node.isArray()) return node.size();
		int len = 0;
		for (;; len++)
			if (!node.has(Integer.toString(len))) return len;
	}

	@SafeVarargs
	public static String simpleJSON(String key, Object value, Tuple2<String, Object>... kvs) {
		Map<String, Object> map = new HashMap<>();
		map.put(key, value);
		for (Tuple2<String, ?> e : kvs)
			map.put(e._1, e._2);
		return JsonSerder.JSON_MAPPER.ser(map).toString();
	}

	@SuppressWarnings("unchecked")
	public static String simpleJSON(Map<String, ?> map) {
		return JsonSerder.JSON_MAPPER.ser((Map<String, Object>) map).toString();
	}
}
