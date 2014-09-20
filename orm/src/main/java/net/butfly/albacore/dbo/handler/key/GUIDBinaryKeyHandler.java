package net.butfly.albacore.dbo.handler.key;

import java.sql.ResultSet;
import java.sql.SQLException;

import net.butfly.albacore.dbo.type.GUID;

public class GUIDBinaryKeyHandler extends BinaryKeyHandlerBase<GUID> {
	@Override
	protected byte[] serialize(GUID key) {
		return key.data();
	}

	@Override
	protected GUID deserialize(byte[] value) {
		return new GUID(value);
	}

	@Override
	public GUID getResult(ResultSet rs, int columnIndex) throws SQLException {
		return new GUID(rs.getBytes(columnIndex));
	}
}
