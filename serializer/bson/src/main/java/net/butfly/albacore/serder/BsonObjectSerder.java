package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.bson.DBEncoder;

public class BsonObjectSerder implements Serder<ByteArrayOutputStream, BSONObject> {
	private static final long serialVersionUID = 6664350391207228363L;
	private static final Logger logger = LoggerFactory.getLogger(BsonObjectSerder.class);

	@Override
	public <T extends ByteArrayOutputStream> BasicBSONObject ser(T from) {
		BasicBSONObject r = new BasicBSONObject();
		r.putAll(new LazyBSONObject(from.toByteArray(), new LazyBSONCallback()));
		return r;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends ByteArrayOutputStream> T der(BSONObject from, TypeToken<T> to) {
		if (null == from) return null;
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			ByteArrayOutputStream bao = new ByteArrayOutputStream();
			try {
				new DBEncoder().writeObject(buf, from);
			} catch (Exception ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
			try {
				bao.write(buf.toByteArray());
			} catch (IOException e) {
				return null;
			}
			return (T) bao;
		} finally {
			buf.close();
		}
	}
}
