package net.butfly.albacore.calculus.marshall.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.BsonUndefined;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

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
import com.mongodb.DBRef;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.calculus.marshall.Marshaller;

public abstract class BsonMarshaller<FK, VK, VV> extends Marshaller<FK, VK, VV> {
	private static final long serialVersionUID = -7385678674433019238L;

	public BsonMarshaller() {
		super();
	}

	public BsonMarshaller(Function<String, String> mapping) {
		super(mapping);
	}

	public static ObjectMapper bsoner = new ObjectMapper(MongoBsonFactory.createFactory()).setPropertyNamingStrategy(
			new UpperCaseWithUnderscoresStrategy()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).disable(
					MapperFeature.USE_GETTERS_AS_SETTERS).disable(SerializationFeature.WRITE_NULL_MAP_VALUES).setSerializationInclusion(
							Include.NON_NULL).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).configure(
									JsonParser.Feature.IGNORE_UNDEFINED, true);
	// .enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)

	@Override
	public final <T> T unmarshall(VV from, Class<T> to) {
		if (null == from) return null;
		return unmarshallFromBSON(decode(from), to);

	}

	@Override
	public final <T> VV marshall(T from) {
		if (null == from) return null;
		return encode(marshallToBSON(from));
	}

	abstract protected BSONObject decode(VV value);

	abstract protected VV encode(BSONObject value);

	private static final class DBEncoder extends DefaultDBEncoder {
		@SuppressWarnings("rawtypes")
		protected void _putObjectField(final String name, final Object initialValue) {
			if ("_transientFields".equals(name)) { return; }
			if (name.contains("\0")) { throw new IllegalArgumentException(
					"Document field names can't have a NULL character. (Bad MapReduceKey: '" + name + "')"); }

			if ("$where".equals(name) && initialValue instanceof String) {
				putCode(name, new Code((String) initialValue));
			}

			Object value = BSON.applyEncodingHooks(initialValue);
			if (value == null || value instanceof BsonUndefined) putNull(name);
			else if (value instanceof Date) putDate(name, (Date) value);
			else if (value instanceof Number) putNumber(name, (Number) value);
			else if (value instanceof Character) putString(name, value.toString());
			else if (value instanceof String) putString(name, value.toString());
			else if (value instanceof ObjectId) putObjectId(name, (ObjectId) value);
			else if (value instanceof Boolean) putBoolean(name, (Boolean) value);
			else if (value instanceof Pattern) putPattern(name, (Pattern) value);
			else if (value instanceof Iterable) putIterable(name, (Iterable) value);
			else if (value instanceof BSONObject) putObject(name, (BSONObject) value);
			else if (value instanceof Map) putMap(name, (Map) value);
			else if (value instanceof byte[]) putBinary(name, (byte[]) value);
			else if (value instanceof Binary) putBinary(name, (Binary) value);
			else if (value instanceof UUID) putUUID(name, (UUID) value);
			else if (value.getClass().isArray()) putArray(name, value);
			else if (value instanceof Symbol) putSymbol(name, (Symbol) value);
			else if (value instanceof BSONTimestamp) putTimestamp(name, (BSONTimestamp) value);
			else if (value instanceof CodeWScope) putCodeWScope(name, (CodeWScope) value);
			else if (value instanceof Code) putCode(name, (Code) value);
			else if (value instanceof DBRef) {
				BSONObject temp = new BasicBSONObject();
				temp.put("$ref", ((DBRef) value).getCollectionName());
				temp.put("$key", ((DBRef) value).getId());
				putObject(name, temp);
			} else if (value instanceof MinKey) putMinKey(name);
			else if (value instanceof MaxKey) putMaxKey(name);
			else if (putSpecial(name, value)) ; // no-op
			else {
				throw new IllegalArgumentException("Can't serialize " + value.getClass());
			}
		}
	}

	@SuppressWarnings("deprecation")
	private <T> T unmarshallFromBSON(BSONObject bson, Class<T> to) {
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			try {
				new DBEncoder().writeObject(buf, bson);
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

	private <T> BSONObject marshallToBSON(T from) {
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

	private static ObjectMapper jsoner = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).disable(
			MapperFeature.USE_GETTERS_AS_SETTERS).disable(SerializationFeature.WRITE_NULL_MAP_VALUES).setSerializationInclusion(
					Include.NON_NULL).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).configure(
							JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true).configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS,
									true);

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

	public static BasicDBObject assembly(String key, Object value) {
		BasicDBObject fd = new BasicDBObject();
		fd.put(key, value);
		return fd;
	}
}
