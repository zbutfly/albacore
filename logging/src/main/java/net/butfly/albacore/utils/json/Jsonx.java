package net.butfly.albacore.utils.json;

import static net.butfly.albacore.utils.json.Jsoner.er;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import javax.json.Json;
import javax.json.stream.JsonGenerator;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class Jsonx {
	public static Map<String, Object> map(String json) {
		throw new UnsupportedOperationException();
	}

	public static String map(Map map) {
		try (StringWriter w = new StringWriter(); JsonGenerator g = Json.createGenerator(w);) {
			if (null == map) g.writeNull();
			else {
				g.writeStartObject();
				try {
					map.forEach((k, v) -> g.write(k.toString(), er.o(v)));
				} finally {
					g.writeEnd();
				}
			}
			return w.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String it(Iterable list) {
		try (StringWriter w = new StringWriter(); JsonGenerator g = Json.createGenerator(w);) {
			if (null == list) g.writeNull();
			else {
				g.writeStartArray();
				try {
					list.forEach((v) -> g.write(er.it((Iterable) v)));
				} finally {
					g.writeEnd();
				}
			}
			return w.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static String json(Object obj) {
		return er.o(obj).toString();
	}
}
