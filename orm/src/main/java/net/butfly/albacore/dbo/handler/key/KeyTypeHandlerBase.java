package net.butfly.albacore.dbo.handler.key;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

@SuppressWarnings("unchecked")
public abstract class KeyTypeHandlerBase<K extends Serializable, S> implements TypeHandler<K> {
	final public void setParameter(PreparedStatement ps, int i, K param, JdbcType type) throws SQLException {
		int sqlType = null == type ? this.getSQLType() : type.ordinal();
		if (param == null) ps.setNull(i, sqlType);
		else ps.setObject(i, this.serialize(param), sqlType);
	}

	@Override
	final public K getResult(ResultSet rs, String columnName) throws SQLException {
		return this.deserialize((S) rs.getObject(columnName));
	}

	@Override
	final public K getResult(CallableStatement cs, int columnIndex) throws SQLException {
		return this.deserialize((S) cs.getObject(columnIndex));
	}

	abstract protected S serialize(K key);

	abstract protected K deserialize(S value);

	abstract protected int getSQLType();
}
