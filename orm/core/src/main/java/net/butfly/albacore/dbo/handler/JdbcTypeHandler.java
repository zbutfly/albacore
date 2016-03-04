package net.butfly.albacore.dbo.handler;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

@SuppressWarnings("unchecked")
public abstract class JdbcTypeHandler<D extends Serializable, S>
		/* extends BaseTypeHandler<K> */ implements TypeHandler<D> {
	final public void setParameter(PreparedStatement ps, int i, D param, JdbcType jdbcType) throws SQLException {
		int sqlType = null == jdbcType ? this.getSQLType().ordinal() : jdbcType.ordinal();
		if (param == null) ps.setNull(i, sqlType);
		else try {
			ps.setObject(i, this.serialize(param), sqlType);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	final public D getResult(ResultSet rs, String columnName) throws SQLException {
		try {
			return this.deserialize((S) rs.getObject(columnName));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	final public D getResult(ResultSet rs, int columnIndex) throws SQLException {
		try {
			return this.deserialize((S) rs.getObject(columnIndex));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	final public D getResult(CallableStatement cs, int columnIndex) throws SQLException {
		try {
			return this.deserialize((S) cs.getObject(columnIndex));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	abstract protected S serialize(D key) throws Exception;

	abstract protected D deserialize(S value) throws Exception;

	abstract protected JdbcType getSQLType();
}
