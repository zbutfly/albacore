package net.butfly.albacore.calculus.marshall.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;

public abstract class BsonMarshaller<FK, VK, VV> extends Marshaller<FK, VK, VV> {
	private static final long serialVersionUID = -7385678674433019238L;
	private static ObjectMapper bsoner = new ObjectMapper(MongoBsonFactory.createFactory())
			.setPropertyNamingStrategy(new UpperCaseWithUnderscoresStrategy()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.disable(MapperFeature.USE_GETTERS_AS_SETTERS).disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
			.setSerializationInclusion(Include.NON_NULL).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
	// .enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)

	@Override
	public final <T extends Factor<T>> T unmarshall(VV from, Class<T> to) {
		if (null == from) return null;
		return unmarshallFromBSON(decode(from), to);

	}

	@Override
	public final <T extends Factor<T>> VV marshall(T from) {
		if (null == from) return null;
		return encode(marshallToBSON(from));
	}

	abstract protected BSONObject decode(VV value);

	abstract protected VV encode(BSONObject value);

	@SuppressWarnings("deprecation")
	private <T extends Factor<T>> T unmarshallFromBSON(BSONObject bson, Class<T> to) {
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			try {
				DefaultDBEncoder.FACTORY.create().writeObject(buf, bson);
			} catch (Exception ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
			try {
				return bsoner.reader(to).readValue(buf.toByteArray());
			} catch (IOException ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
		} finally {
			buf.close();
		}
	}

	private <T extends Factor<T>> BSONObject marshallToBSON(T from) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			bsoner.writer().writeValue(baos, from);
		} catch (IOException e) {
			logger.error("BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		DBObject r = new BasicDBObject();
		r.putAll(new LazyDBObject(baos.toByteArray(), new LazyBSONCallback()));
		return r;
	}

	private static ObjectMapper jsoner = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.disable(MapperFeature.USE_GETTERS_AS_SETTERS).disable(SerializationFeature.WRITE_NULL_MAP_VALUES)
			.setSerializationInclusion(Include.NON_NULL).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
			.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
			.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);

	public BSONObject bsonFromJSON(String json) {
		try {
			return new BasicDBObject(jsoner.readValue(json, new TypeReference<Map<String, Object>>() {}));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String jsonFromBSON(BSONObject bson) {
		try {
			return jsoner.writeValueAsString(bson.toMap());
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String... args) {
		MongoMarshaller m = new MongoMarshaller();
		BSONObject b = m.bsonFromJSON("{$and: [{a: 1, c:true, dd: {a: \"32\", d: [13, 2]}}, {b: 'asdfdsf'}]}");
		List<Object> and = (List<Object>) b.get("$and");
		Map<String, Object> item = (Map<String, Object>) and.get(0);
		Map<String, Object> dd = (Map<String, Object>) item.get("dd");
		List<Object> d = (List<Object>) dd.get("d");
		Integer i = (Integer) d.get(0);
		assert (i == 13);

		and.add(new byte[] { 1, 2, 3 });
		and.add(new boolean[] { false, true, false });
		and.add(123456789);
		System.out.println(m.jsonFromBSON(b));
	}
}
