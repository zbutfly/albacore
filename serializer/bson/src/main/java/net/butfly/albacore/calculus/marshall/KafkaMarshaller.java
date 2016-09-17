package net.butfly.albacore.calculus.marshall;

import java.util.function.Function;

import org.bson.BSON;
import org.bson.BSONObject;

@Deprecated
public class KafkaMarshaller extends BsonMarshaller<String, String, byte[]> {
	private static final long serialVersionUID = -4471098188111221100L;

	public KafkaMarshaller() {
		super();
	}

	public KafkaMarshaller(Function<String, String> mapping) {
		super(mapping);
	}

	@Override
	protected BSONObject decode(byte[] value) {
		return (BSONObject) BSON.decode(value).get("value");
	}

	@Override
	protected byte[] encode(BSONObject value) {
		return BSON.encode(value);
	}
}
