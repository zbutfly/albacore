package net.butfly.albacore.serder.bson;

import java.io.IOException;
import java.util.Map;

import org.apache.http.entity.ContentType;
import org.bson.BSONObject;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObject;

import net.butfly.albacore.serder.ContentSerderBase;
import net.butfly.albacore.serder.TextSerder;
import net.butfly.albacore.serder.TextSerderBase;
import net.butfly.albacore.serder.WrapSerder;
import net.butfly.bus.serialize.Jsons;

public class BsonObjectJsonSerder extends ContentSerderBase<Object, CharSequence> implements
		WrapSerder<Object, BSONObject, CharSequence, BsonObjectSerder, BsonObjectJsonSerder.BsonObjectJsonSerderImpl> {
	private static final long serialVersionUID = 6664350391207228363L;

	private BsonObjectSerder s1;
	private BsonObjectJsonSerderImpl s2;

	public BsonObjectJsonSerder() {
		super(ContentType.APPLICATION_JSON);
		this.s1 = new BsonObjectSerder();
		this.s2 = new BsonObjectJsonSerderImpl();
	}

	public BsonObjectJsonSerder(ContentType contentType) {
		super(contentType);
		this.s1 = new BsonObjectSerder();
		this.s2 = new BsonObjectJsonSerderImpl(contentType);
	}

	@Override
	public BsonObjectSerder s1() {
		return s1;
	}

	@Override
	public BsonObjectJsonSerderImpl s2() {
		return s2;
	}

	@Override
	public Class<BSONObject> medium() {
		return BSONObject.class;
	}

	@Override
	public Object[] deserialize(CharSequence dst, Class<?>[] types) {
		// TODO Auto-generated method stub
		return null;
	}

	protected static class BsonObjectJsonSerderImpl extends TextSerderBase<BSONObject> implements TextSerder<BSONObject> {
		private static final long serialVersionUID = 1066493022911366724L;
		private static ObjectMapper jsoner = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).disable(
				MapperFeature.USE_GETTERS_AS_SETTERS).disable(SerializationFeature.WRITE_NULL_MAP_VALUES).setSerializationInclusion(
						Include.NON_NULL).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).configure(
								JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true).configure(
										JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);

		public BsonObjectJsonSerderImpl() {
			super(ContentType.APPLICATION_JSON);
		}

		public BsonObjectJsonSerderImpl(ContentType contentType) {
			super(contentType);
		}

		@Override
		public CharSequence serialize(Object from) {
			try {
				return jsoner.writeValueAsString(((BSONObject) from).toMap());
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Object deserialize(CharSequence from, Class<?> to) {
			try {
				return new BasicDBObject(jsoner.readValue(from.toString(), new TypeReference<Map<String, Object>>() {}));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Object[] deserialize(CharSequence from, Class<?>[] tos) {
			try {
				JsonNode[] n = Jsons.dearray(from);
				if (n == null) return null;
				Object[] r = new Object[Math.min(tos.length, n.length)];
				for (int i = 0; i < r.length; i++) {
					r[i] = deserializeT(n[i].asText(), tos[i]);
					new BasicDBObject();
					 Jsons.mapper.treeToValue(n[i], tos[i]);
				}
				return r;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
