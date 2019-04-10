package net.butfly.albacore.utils.json;

import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createValue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

@SuppressWarnings({ "unchecked", "rawtypes" })
class Jsoner {
	public final static Jsoner er = new Jsoner();
	public final Map<String, Object> config = new ConcurrentHashMap<>();

	JsonValue m(Map map) {
		if (null == map) return JsonValue.NULL;
		JsonObjectBuilder b = createObjectBuilder(config);
		map.forEach((k, v) -> b.add(k.toString(), o(v)));
		return b.build();
	}

	JsonValue c(Collection list) {
		if (null == list) return JsonValue.NULL;
		return list.isEmpty() ? JsonValue.EMPTY_JSON_ARRAY : it((Iterable) list);
	}

	JsonValue it(Iterable list) {
		if (null == list) return JsonValue.NULL;
		JsonArrayBuilder b = createArrayBuilder();
		list.forEach((v) -> b.add(o(v)));
		return b.build();
	}

	JsonValue n(Number n) {
		if (null == n) return JsonValue.NULL;
		if (n instanceof Long) return createValue(n.longValue());
		if (n instanceof Integer) return createValue(n.intValue());
		if (n instanceof BigInteger) return createValue((BigInteger) n);
		if (n instanceof BigDecimal) return createValue((BigDecimal) n);
		return createValue(n.doubleValue());
	}

	JsonValue o(Object obj) {
		if (null == obj) return JsonValue.NULL;
		if (obj instanceof Map) return m((Map) obj);
		Class<? extends Object> c = obj.getClass();
		if (Void.class.isAssignableFrom(c)) return JsonValue.NULL;
		if (CharSequence.class.isAssignableFrom(c) || Character.class.isAssignableFrom(c)) return createValue(obj.toString());
		if (Number.class.isAssignableFrom(c)) return n((Number) obj);
		if (Boolean.class.isAssignableFrom(c)) return ((Boolean) obj).booleanValue() ? JsonValue.TRUE : JsonValue.FALSE;
		if (c.isArray()) return c(Arrays.asList((Object[]) obj));
		if (Iterable.class.isAssignableFrom(c)) return it((Iterable) obj);

		throw new UnsupportedOperationException("Object not supported");
	}
}
