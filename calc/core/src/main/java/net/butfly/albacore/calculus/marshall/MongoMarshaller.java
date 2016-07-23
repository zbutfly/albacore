package net.butfly.albacore.calculus.marshall;

import org.bson.BSONObject;

import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.bson.BsonMarshaller;

public class MongoMarshaller extends BsonMarshaller<Object, Object, BSONObject> {
	private static final long serialVersionUID = 8467183278278572295L;

	public MongoMarshaller() {
		super();
	}

	public MongoMarshaller(Func<String, String> mapping) {
		super(mapping);
	}

	@Override
	protected BSONObject decode(BSONObject value) {
		return value;
	}

	@Override
	protected BSONObject encode(BSONObject value) {
		return value;
	}
};