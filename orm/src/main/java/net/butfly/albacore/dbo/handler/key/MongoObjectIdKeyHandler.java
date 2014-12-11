package net.butfly.albacore.dbo.handler.key;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.bson.types.ObjectId;

public class MongoObjectIdKeyHandler extends KeyTypeHandlerBase<ObjectId, String> {
	@Override
	protected String serialize(ObjectId key) {
		return key.toHexString();
	}

	@Override
	protected ObjectId deserialize(String value) {
		return new ObjectId(value);
	}

	@Override
	protected int getSQLType() {
		return JdbcType.VARCHAR.ordinal();
	}

	@Override
	public ObjectId getResult(ResultSet rs, int columnIndex) throws SQLException {
		return new ObjectId(rs.getString(columnIndex));
	}
}
