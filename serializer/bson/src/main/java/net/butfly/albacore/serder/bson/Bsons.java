package net.butfly.albacore.serder.bson;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObject;

import net.butfly.albacore.calculus.marshall.bson.bson4jackson.MongoBsonFactory;
import net.butfly.albacore.serder.json.UpperCaseWithUnderscoresStrategy;
import net.butfly.albacore.utils.Utils;

public final class Bsons extends Utils {
	public static ObjectMapper bsoner = new ObjectMapper(MongoBsonFactory.createFactory())//
			.setPropertyNamingStrategy(new UpperCaseWithUnderscoresStrategy())//
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)//
			.disable(MapperFeature.USE_GETTERS_AS_SETTERS)//
			.disable(SerializationFeature.WRITE_NULL_MAP_VALUES)//
			.setSerializationInclusion(Include.NON_NULL)//
			.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)//
			.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);

	public static BasicDBObject assembly(String key, Object value) {
		BasicDBObject fd = new BasicDBObject();
		fd.put(key, value);
		return fd;
	}
}
