package net.butfly.albacore.calculus.marshall;

import org.bson.BSON;
import org.bson.BSONObject;

import net.butfly.albacore.calculus.marshall.bson.BsonMarshaller;

public class KafkaMarshaller extends BsonMarshaller<String, String, byte[]> {
	private static final long serialVersionUID = -4471098188111221100L;

	@Override
	protected BSONObject decode(byte[] value) {
		return (BSONObject) BSON.decode(value).get("value");
	}

	@Override
	protected byte[] encode(BSONObject value) {
		return BSON.encode(value);
	}
}
