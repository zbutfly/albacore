package net.butfly.albacore.dbo.handler.key;

import java.io.Serializable;
import java.sql.Types;

public abstract class BinaryKeyHandlerBase<K extends Serializable> extends KeyTypeHandlerBase<K, byte[]> {
	@Override
	protected int getSQLType() {
		return Types.BINARY;
	}
}
