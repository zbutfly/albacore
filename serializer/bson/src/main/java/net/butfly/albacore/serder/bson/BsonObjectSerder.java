package net.butfly.albacore.serder.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BasicBSONList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.serder.ArrableSerder;
import net.butfly.albacore.serder.BeanSerder;

public class BsonObjectSerder implements BeanSerder<BSONObject>, ArrableSerder<Object, BSONObject> {
	private static final long serialVersionUID = 6664350391207228363L;
	private static final Logger logger = LoggerFactory.getLogger(BsonObjectSerder.class);

	@Override
	public <T> BSONObject ser(T from) {
		if (null == from) return null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			Bsons.bsoner.writer().writeValue(baos, from);
		} catch (IOException e) {
			logger.error("BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		DBObject r = new BasicDBObject();
		r.putAll(new LazyDBObject(baos.toByteArray(), new LazyBSONCallback()));
		return r;
	}

	@Override
	public <T> T der(BSONObject from, Class<T> to) {
		if (null == from) return null;
		return fromBSON(from, to);
	}

	@Override
	public Object[] der(BSONObject from, Class<?>... tos) {
		if (null == from) return null;
		if (!(from instanceof BasicBSONList)) return new Object[] { fromBSON(from, tos[0]) };
		BasicBSONList bl = (BasicBSONList) from;
		Object[] r = new Object[Math.min(bl.size(), tos.length)];
		for (int i = 0; i < r.length; i++)
			r[i] = fromBSON((BSONObject) bl.get(Integer.toString(i)), tos[i]);
		return r;
	}

	@SuppressWarnings("deprecation")
	private <T> T fromBSON(BSONObject bson, Class<T> to) {
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			try {
				new DBEncoder().writeObject(buf, bson);
			} catch (Exception ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
			try {
				return Bsons.bsoner.reader(to).readValue(buf.toByteArray());
			} catch (IOException ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
		} finally {
			buf.close();
		}
	}
}
