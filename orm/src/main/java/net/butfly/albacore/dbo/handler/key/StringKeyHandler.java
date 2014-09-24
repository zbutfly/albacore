package net.butfly.albacore.dbo.handler.key;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;

public class StringKeyHandler extends KeyTypeHandlerBase<String, String> {
	@Override
	protected String serialize(String key) {
		return key;
	}

	@Override
	protected String deserialize(String value) {
		return value;
	}

	@Override
	protected int getSQLType() {
		return JdbcType.VARCHAR.ordinal();
	}

	@Override
	public String getResult(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getString(columnIndex);
	}
}
