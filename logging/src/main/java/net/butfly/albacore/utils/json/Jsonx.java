package net.butfly.albacore.utils.json;

import static net.butfly.albacore.utils.json.Jsoner.er;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;

import net.butfly.albacore.utils.logger.Logger;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class Jsonx {
	static final Logger logger = Logger.getLogger(Jsonx.class);

	private static class NameValue {
		private final String name;
		private final Object value;

		public NameValue(String name, Object value) {
			super();
			this.name = name;
			this.value = value;
		}

		@Override
		public String toString() {
			return "<" + name + ": " + value + ">";
		}
	}

	public static Object map(String json) {
		try (StringReader in = new StringReader(json); JsonParser p = Json.createParser(in);) {
			String k = null;
			Stack<NameValue> stack = new Stack<>();
			NameValue v;
			while (p.hasNext()) {
				switch (p.next()) {
				case START_ARRAY:
					stack.push(new NameValue(k, new ArrayList<>()));
					k = null;
					break;
				case START_OBJECT:
					stack.push(new NameValue(k, new HashMap<>()));
					k = null;
					break;
				case END_ARRAY:
					if (peek(stack, v = stack.pop())) return v.value;
					k = null;
					break;
				case END_OBJECT:
					if (peek(stack, v = stack.pop())) return v.value;
					k = null;
					break;
				case KEY_NAME:
					k = p.getString();
					break;
				case VALUE_STRING:
					if ((peek(stack, v = new NameValue(k, p.getString())))) return v.value;
					k = null;
					break;
				case VALUE_FALSE:
					if ((peek(stack, v = new NameValue(k, false)))) return v.value;
					k = null;
					break;
				case VALUE_TRUE:
					if ((peek(stack, v = new NameValue(k, true)))) return v.value;
					k = null;
					break;
				case VALUE_NUMBER:
					String ns = p.getString();
					if (ns.indexOf('.') >= 0) {
						double n = new BigDecimal(ns).doubleValue();
						if ((peek(stack, new NameValue(k, n)))) return n;
					} else {
						long n = Long.parseLong(ns);
						if ((peek(stack, new NameValue(k, n)))) return n;
					}
					k = null;
					break;
				case VALUE_NULL: // don't set anything
					if ((peek(stack, new NameValue(k, null)))) return null;
					k = null;
					break;
				}
			}
			throw new IllegalArgumentException("Invalid json.");
		}
	}

	private static boolean peek(Stack<NameValue> stack, NameValue v) {
		if (!stack.isEmpty()) {
			NameValue top = stack.peek();
			Class<?> c = top.value.getClass();
			if (Map.class.isAssignableFrom(c)) {
				if (null == v.name) throw new IllegalArgumentException("Value found but no key existed.");
				((Map<String, Object>) top.value).put(v.name, v.value);
			} else if (List.class.isAssignableFrom(c)) { //
				if (null != v.name) logger.error("Json item in list should not had name.");
				((List<Object>) top.value).add(v.value);
			}
			return false;
		} else {
			// if (p.hasNext()) logger.error("Json parsing finish but content existed, return current result.");
			return true;
		}
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
					g.flush();
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
					g.flush();
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

	public static void main(String... args) {
		Object m = map(
				"{\"errcode\":\"000200\",\"errmsg\":\"success\",\"responseObject\":{\"partitionColumnList\":[{\"partitionFieldType\":\"string\",\"srcFieldName\":\"ETL_TIMESTAMP_dt\",\"level\":1,\"srcFieldType\":\"date\",\"partitionFormat\":\"yyyy\",\"partitionFieldName\":\"year\"},{\"partitionFieldType\":\"string\",\"srcFieldName\":\"ETL_TIMESTAMP_dt\",\"level\":2,\"srcFieldType\":\"date\",\"partitionFormat\":\"MM\",\"partitionFieldName\":\"month\"},{\"partitionFieldType\":\"string\",\"srcFieldName\":\"ETL_TIMESTAMP_dt\",\"level\":3,\"srcFieldType\":\"date\",\"partitionFormat\":\"dd\",\"partitionFieldName\":\"day\"}],\"partitionConfigList\":[{\"maxTimeInterval\":\"1\",\"maxRecordSize\":1000,\"maxFileSize\":128}]}}");

		System.out.print(m);
	}

	public static String pretty(String json) {
		return json;
	}
}
