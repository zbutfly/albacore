package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import net.butfly.albacore.serder.json.Jsons;

public class BsonSerder<E> implements BinarySerder<E>, BeanSerder<E, byte[]> {
	private static final long serialVersionUID = 6664350391207228363L;
	public static final BsonSerder<Object> DEFAULT_OBJ = new BsonSerder0();
	public static final BsonMapSerder DEFAULT_MAP = new BsonMapSerder();

	@Override
	public byte[] ser(E from) {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			Jsons.bsoner.writeValue(baos, from);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public Object[] der(byte[] from, Class<?>... tos) {
		JsonNode[] n;
		try {
			n = Jsons.array(Jsons.bsoner.readTree(from));
		} catch (IOException e) {
			return null;
		}
		Object[] r = new Object[Math.min(n.length, tos.length)];
		for (int i = 0; i < r.length; i++)
			r[i] = single(n[i], tos[i]);
		return r;
	}

	protected <T> T single(JsonNode node, Class<T> tos) {
		try {
			return Jsons.bsoner.readValue(Jsons.bsoner.treeAsTokens(node), tos);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T extends E> T der(byte[] from, Class<T> to) {
		if (null == from) return null;
		try (ByteArrayInputStream bais = new ByteArrayInputStream(from);) {
			return Jsons.bsoner.readValue(bais, to);
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static final class BsonMapSerder extends BsonSerder<Map<String, Object>> implements
			ClassInfoSerder<Map<String, Object>, byte[]> {
		private static final long serialVersionUID = 6664350391207228363L;
		private static final JavaType t = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class);

		@Override
		public Map<String, Object>[] der(byte[] from, Class<?>... tos) {
			return (Map<String, Object>[]) super.der(from, tos);
		}

		@Override
		protected <T> T single(JsonNode node, Class<T> c) {
			return (T) super.single(node, Map.class);
		}

		@Override
		public <T extends Map<String, Object>> T der(byte[] from) {
			if (null == from) return null;
			try (ByteArrayInputStream bais = new ByteArrayInputStream(from);) {
				return Jsons.bsoner.readValue(bais, t);
			} catch (IOException e) {
				return null;
			}
		}

		@Override
		public <T extends Map<String, Object>> T der(byte[] from, Class<T> to) {
			return ((ClassInfoSerder<Map<String, Object>, byte[]>) this).der(from);
		}
	}

	public static final class BsonSerder0 extends BsonSerder<Object> {
		private static final long serialVersionUID = 1L;
	}
}
