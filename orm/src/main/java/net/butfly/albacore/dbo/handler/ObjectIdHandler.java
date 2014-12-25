package net.butfly.albacore.dbo.handler;

import org.apache.ibatis.type.JdbcType;
import org.bson.types.ObjectId;

public class ObjectIdHandler extends JdbcTypeHandler<ObjectId, String> {
	@Override
	protected String serialize(ObjectId key) {
		return key.toHexString();
	}

	@Override
	protected ObjectId deserialize(String value) {
		return new ObjectId(value);
	}

	@Override
	protected JdbcType getSQLType() {
		return JdbcType.VARCHAR;
	}
}
