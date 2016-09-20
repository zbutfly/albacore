package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.serder.bson.DBEncoder;
import net.butfly.albacore.serder.json.Jsons;

public class BsonObjectBeanSerder implements Serder<Object, BSONObject>, BeanSerder<BSONObject> {
	private static final long serialVersionUID = 8050515547072577482L;
	private static final Logger logger = LoggerFactory.getLogger(BsonObjectBeanSerder.class);

	@Override
	public <T> BasicBSONObject ser(T from) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			Jsons.bsoner.writer().writeValue(baos, from);
		} catch (IOException e) {
			logger.error("BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		BasicDBObject r = new BasicDBObject();
		r.putAll(new LazyDBObject(baos.toByteArray(), new LazyBSONCallback()));
		return r;
	}

	@Override
	public <T> T der(BSONObject from, Class<T> to) {
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			try {
				new DBEncoder().writeObject(buf, from);
			} catch (Exception ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
			try {
				return Jsons.bsoner.readValue(buf.toByteArray(), to);
			} catch (IOException ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
		} finally {
			buf.close();
		}
	}
}
