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
	public static ObjectMapper mapper = new ObjectMapper()//
			.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)//
			.configure(JsonParser.Feature.IGNORE_UNDEFINED, true)//
			.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)//
			.enable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)//
			.enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)//
			.enable(SerializationFeature.WRITE_ENUMS_USING_INDEX)//
			.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)//
	;
	private static JsonFactory bsoner_fact = new BsonFactory()//
			// .enable(BsonGenerator.Feature.ENABLE_STREAMING)//cause EOF
			.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)//
	;
	public static ObjectMapper bsoner = new ObjectMapper(bsoner_fact)//
			.setPropertyNamingStrategy(new UpperCaseWithUnderscoresStrategy())//
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)//
			.disable(MapperFeature.USE_GETTERS_AS_SETTERS)//
			.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)//
			.setSerializationInclusion(Include.NON_NULL)//
			.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)//
			.configure(JsonParser.Feature.IGNORE_UNDEFINED, true)//
	;

	public static <T> T parse(JsonNode node, Class<T> to) {
		try {
			return mapper.treeToValue(node, to);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static JsonNode[] dearray(JsonNode node) throws JsonProcessingException, IOException {
		// final JsonNode node = Jsons.mapper.readTree(src.toString());
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
	public static String simpleJSON(String key, Object value, Tuple2<String, Object>... kvs) {
		Map<String, Object> map = new HashMap<>();
		map.put(key, value);
		for (Tuple2<String, ?> e : kvs)
			map.put(e._1, e._2);
		return jsd.ser(map);
	}

	public static String simpleJSON(Map<String, ?> map) {
		return jsd.ser(map);
	}
}
