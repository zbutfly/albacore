package net.butfly.albacore.serializer.bson;

import com.mongodb.BasicDBObject;

import net.butfly.albacore.utils.Utils;

public final class Bsons extends Utils {
	public static BasicDBObject assembly(String key, Object value) {
		BasicDBObject fd = new BasicDBObject();
		fd.put(key, value);
		return fd;
	}
}
