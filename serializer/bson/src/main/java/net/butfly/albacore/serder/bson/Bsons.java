package net.butfly.albacore.serder.bson;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObject;

import net.butfly.albacore.calculus.marshall.bson.bson4jackson.MongoBsonFactory;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.serder.JsonSerder;
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

	/******************* test ***************/
	@JsonAutoDetect(fieldVisibility = Visibility.ANY)
	static class A {
		private boolean is = false;
		private String name = "test";
		private int[] values = new int[] { 1, 3, 9, 16 };
	}
	private static final JsonSerder jsd = new JsonSerder();
	private static final BsonSerder bsd = new BsonSerder();

	public static void main(String... a) {
		A o = new A();
		String s = jsd.ser(o);
		System.out.println("JSON:" + s);
		o = jsd.der(s, A.class);
		System.out.println("After JSON back: " + o.name);
		byte[] b = bsd.ser(o);
		o = bsd.der(b, A.class);
		System.out.println("After BSON back: " + o.name);
		Object[] args = jsd.der(jsd.ser(new Object[] { o, 1, true }), A.class, int.class, boolean.class);
		System.out.println("After JSON args: " + ((A) args[0]).name);
		args = bsd.der(bsd.ser(new Object[] { o, 1, true }), A.class, int.class, boolean.class);
		System.out.println("After BSON args: " + ((A) args[0]).name);
	}
}
