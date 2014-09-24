package net.butfly.albacore.dbo.handler;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.butfly.albacore.support.EnumSupport;
import net.butfly.albacore.utils.EnumUtils;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public abstract class EnumTypeHandlerBase<E extends EnumSupport<E>> implements TypeHandler<E> {
	@Override
	public void setParameter(PreparedStatement ps, int i, E parameter, JdbcType jdbcType) throws SQLException {
		ps.setNull(i, null == parameter ? jdbcType.ordinal() : (parameter).value());
	}

	@Override
	public E getResult(ResultSet rs, String columnName) throws SQLException {
		Integer value = rs.getInt(columnName);
		return null == value ? null : EnumUtils.valueOf(this.getEnumClass(), value);
	}

	@Override
	public E getResult(CallableStatement cs, int columnIndex) throws SQLException {
		Integer value = cs.getInt(columnIndex);
		return null == value ? null : EnumUtils.valueOf(this.getEnumClass(), value);
	}

	abstract protected Class<E> getEnumClass();
}
