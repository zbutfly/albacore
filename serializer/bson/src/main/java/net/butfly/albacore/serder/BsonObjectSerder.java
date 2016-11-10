package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;

import com.mongodb.LazyDBObject;

import net.butfly.albacore.serder.bson.DBEncoder;
import net.butfly.albacore.serder.support.BeanSerder;
import net.butfly.albacore.serder.support.ContentTypeSerder;
import net.butfly.albacore.serder.support.ContentTypeSerderBase;
import net.butfly.albacore.serder.support.ContentTypes;
import net.butfly.albacore.utils.Reflections;

public final class BsonObjectSerder extends ContentTypeSerderBase implements Serder<BSONObject, byte[]>, ContentTypeSerder {
	private static final long serialVersionUID = 6664350391207228363L;
	public static final BsonObjectSerder DEFAULT = new BsonObjectSerder();

	public BsonObjectSerder() {
		this.contentType = ContentTypes.APPLICATION_BSON;
	}

	@Override
	public byte[] ser(BSONObject from) {
		if (null == from) return null;
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			ByteArrayOutputStream bao = new ByteArrayOutputStream();
			try {
				new DBEncoder().writeObject(buf, from);
			} catch (Exception ex) {
				return null;
			}
			try {
				bao.write(buf.toByteArray());
			} catch (IOException e) {
				return null;
			}
			return bao.toByteArray();
		} finally {
			buf.close();
		}
	}

	@Override
	public <T extends BSONObject> T der(byte[] from, Class<T> to) {
		if (!BasicBSONObject.class.isAssignableFrom(to)) throw new UnsupportedOperationException();
		T r = Reflections.construct(to);
		r.putAll(new LazyBSONObject(from, new LazyBSONCallback()));
		return r;
	}

	public static class MongoSerder implements BeanSerder<Object, BSONObject> {
		private static final long serialVersionUID = 8050515547072577482L;
		public static final MongoSerder DEFAULT = new MongoSerder();

		@Override
		public BSONObject ser(Object from) {
			BSONObject origin = new LazyDBObject(BsonSerder.DEFAULT_OBJ.ser(from), new LazyBSONCallback());
			BSONObject mapped = new BasicBSONObject();
			for (String key : origin.keySet())
				mapped.put(mappingFieldName(key), origin.get(key));
			return mapped;
		}

		@Override
		public <T> T der(BSONObject from, Class<T> to) {
			BSONObject mapped = new BasicBSONObject();
			for (String prop : from.keySet()) {
				String fieldName = unmappingFieldName(to, prop);
				mapped.put(fieldName, processEmbedded(from.get(prop), Reflections.getDeclaredField(to, fieldName).getType()));
			}
			try (OutputBuffer buf = new BasicOutputBuffer();) {
				new DBEncoder().writeObject(buf, mapped);
				return BsonSerder.DEFAULT_OBJ.der(buf.toByteArray(), to);
			}
		}

		private Object processEmbedded(Object fieldValue, Class<?> fieldClass) {
			if (null == fieldValue) return null;
			if (fieldValue instanceof BSONObject) return der((BSONObject) fieldValue, fieldClass);
			return fieldValue;
		}
	}
}
