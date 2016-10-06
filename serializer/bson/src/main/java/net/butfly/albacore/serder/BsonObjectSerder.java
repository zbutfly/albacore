package net.butfly.albacore.serder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.LazyBSONCallback;
import org.bson.LazyBSONObject;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import net.butfly.albacore.utils.logger.Logger;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.bson.DBEncoder;
import net.butfly.albacore.serder.support.ByteArray;

public class BsonObjectSerder implements Serder<ByteArray, BSONObject> {
	private static final long serialVersionUID = 6664350391207228363L;
	private static final Logger logger = Logger.getLogger(BsonObjectSerder.class);

	@Override
	public <T extends ByteArray> BasicBSONObject ser(T from) {
		BasicBSONObject r = new BasicBSONObject();
		r.putAll(new LazyBSONObject(from.get(), new LazyBSONCallback()));
		return r;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends ByteArray> T der(BSONObject from, TypeToken<T> to) {
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
			return (T) new ByteArray(bao.toByteArray());
		} finally {
			buf.close();
		}
	}
}
