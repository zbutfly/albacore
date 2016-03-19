package net.butfly.albacore.calculus.marshall;

import org.bson.BSONObject;

import net.butfly.albacore.calculus.marshall.bson.BsonMarshaller;

public class MongoMarshaller extends BsonMarshaller<Object, BSONObject> {
	private static final long serialVersionUID = 8467183278278572295L;

	@Override
	public String unmarshallId(Object id) {
		return null == id ? null : id.toString();
	}

	@Override
	public Object marshallId(String id) {
		return id;
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