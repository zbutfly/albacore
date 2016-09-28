package net.butfly.bus.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.serder.support.ByteArray;
import net.butfly.albacore.utils.CaseFormat;

public class JacksonTest {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String... arg) throws IOException {
		final JsonSerder jsonSerder = new JsonSerder().mapping(CaseFormat.NO_CHANGE);
		final BsonSerder bsonSerder = new BsonSerder().mapping(CaseFormat.NO_CHANGE);

		Bean obj = new Bean();
		String json = jsonSerder.ser(obj);
		System.out.println("Bean => JSON: " + json);
		System.out.println("Bean => JSON title: " + obj.titles() + " => " + jsonSerder.der(json, TypeToken.of(Bean.class)).titles());
		System.out.println();

		ByteArray bson = bsonSerder.ser(obj);
		System.out.println("Bean => BSON length: " + bson.get().length);
		System.out.println("BSON => Bean title: " + obj.titles() + " => " + bsonSerder.der(bson, TypeToken.of(Bean.class)).titles());
		System.out.println();

		Object[] args = new Object[] { obj, 1, true };
		System.out.println("Orig args [title: " + obj.titles() + " => " + ((Bean) args[0]).titles() + "], " + args[1] + ", " + args[2]);
		args = jsonSerder.der(jsonSerder.ser(args), TypeToken.of(Bean.class), TypeToken.of(int.class), TypeToken.of(boolean.class));
		System.out.println("JSON args [title: " + obj.titles() + " => " + ((Bean) args[0]).titles() + "], " + args[1] + ", " + args[2]);
		args = bsonSerder.der(bsonSerder.ser(new Object[] { obj, 1, true }), TypeToken.of(Bean.class), TypeToken.of(int.class), TypeToken
				.of(boolean.class));
		System.out.println("BSON args [title: " + obj.titles() + " => " + ((Bean) args[0]).titles() + "], " + args[1] + ", " + args[2]);
		System.out.println();

		Map<String, ?> map = map();
		System.out.println("Origin Map: " + map + ", title: " + ((Bean) map.get("object")).titles());
		json = jsonSerder.ser(map);
		System.out.println("Map => JSON: " + json);
		map = jsonSerder.der(json, new TypeToken<Map<String, ?>>() {
			private static final long serialVersionUID = -4110131747435668077L;
		});
		System.out.println("JSON => Map: " + map.toString());
		System.out.println("JSON => Map sub title: " + ((Map) ((Map) map.get("object")).get("bean")).get("title"));
		System.out.println();

		map = map();
		System.out.println("Origin Map: " + map + ", title: " + ((Bean) map.get("object")).titles());
		bson = bsonSerder.ser(map);
		System.out.println("Map => BSON length: " + bson.get().length);
		map = bsonSerder.der(bson, TypeToken.of(Map.class));
		System.out.println("BSON => Map sub title: " + ((Map) ((Map) map.get("object")).get("bean")).get("title"));
		System.out.println();
	}

	private static Map<String, ?> map() {
		Map<String, Object> map = new HashMap<>();
		map.put("name", RandomStringUtils.randomAlphabetic(8));
		map.put("count", (int) (Math.random() * 10));
		map.put("object", new Bean());
		return map;
	}
}
