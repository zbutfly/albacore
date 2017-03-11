package net.butfly.bus.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.serder.JsonSerder;

public class JacksonTest {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String... arg) throws IOException {
		final JsonSerder jsonSerder = JsonSerder.JSON_OBJECT;

		Bean obj = new Bean();
		String json = jsonSerder.ser(obj);
		System.out.println("Bean => JSON: " + json);
		Bean b = (Bean) jsonSerder.der(json, Bean.class);
		System.out.println("Bean => JSON title: " + obj.titles() + " => " + b.titles());
		System.out.println();

		byte[] bson = BsonSerder.DEFAULT_OBJ.ser(obj);
		System.out.println("Bean => BSON length: " + bson.length);
		System.out.println("BSON => Bean title: " + obj.titles() + " => " + BsonSerder.DEFAULT_OBJ.der(bson, Bean.class).titles());
		System.out.println();

		Object[] args = new Object[] { obj, 1, true };
		System.out.println("Orig args [title: " + obj.titles() + " => " + ((Bean) args[0]).titles() + "], " + args[1] + ", " + args[2]);
		args = jsonSerder.der(jsonSerder.ser(args), Bean.class, int.class, boolean.class);
		System.out.println("JSON args [title: " + obj.titles() + " => " + ((Bean) args[0]).titles() + "], " + args[1] + ", " + args[2]);
		args = BsonSerder.DEFAULT_OBJ.der(BsonSerder.DEFAULT_OBJ.ser(new Object[] { obj, 1, true }), Bean.class, int.class, boolean.class);
		System.out.println("BSON args [title: " + obj.titles() + " => " + ((Bean) args[0]).titles() + "], " + args[1] + ", " + args[2]);
		System.out.println();

		Map<String, Object> map = map();
		System.out.println("Origin Map: " + map + ", title: " + ((Bean) map.get("object")).titles());
		json = jsonSerder.ser(map);
		System.out.println("Map => JSON: " + json);
		map = JsonSerder.JSON_MAPPER.der(json);
		System.out.println("JSON => Map: " + map.toString());
		System.out.println("JSON => Map sub title: " + ((Map) ((Map) map.get("object")).get("bean")).get("title"));
		System.out.println();

		map = map();
		System.out.println("Origin Map: " + map + ", title: " + ((Bean) map.get("object")).titles());
		bson = BsonSerder.DEFAULT_MAP.ser(map);
		System.out.println("Map => BSON length: " + bson.length);
		map = BsonSerder.DEFAULT_MAP.der(bson);
		System.out.println("BSON => Map sub title: " + ((Map) ((Map) map.get("object")).get("bean")).get("title"));
		System.out.println();
	}

	static Random r = new Random();

	private static Map<String, Object> map() {
		Map<String, Object> map = new HashMap<>();
		map.put("name", Double.toHexString(r.nextGaussian()));
		map.put("count", (int) (Math.random() * 10));
		map.put("object", new Bean());
		return map;
	}
}
