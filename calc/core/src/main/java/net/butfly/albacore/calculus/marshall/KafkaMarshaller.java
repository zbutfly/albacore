package net.butfly.albacore.calculus.marshall;

import org.bson.BSON;
import org.bson.BSONObject;

import net.butfly.albacore.calculus.marshall.bson.BsonMarshaller;

public class KafkaMarshaller extends BsonMarshaller<byte[], String> {
	private static final long serialVersionUID = -4471098188111221100L;

	@Override
	public String unmarshallId(String id) {
		return id;
	}

	@Override
	public String marshallId(String id) {
		return id;
	}

	@Override
	protected BSONObject encode(byte[] value) {
		return (BSONObject) BSON.decode(value).get("value");
	}

	@Override
	protected byte[] decode(BSONObject value) {
		return BSON.encode(value);
	}
}
