package net.butfly.albacore.dbo.handler;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.butfly.albacore.utils.GenericUtils;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public abstract class SimpleEnumTypeHandlerBase<E extends Enum<E>> implements TypeHandler<E> {
	private Class<E> clazz;

	protected int sqlType() {
		return JdbcType.VARCHAR.TYPE_CODE;
	}

	@SuppressWarnings("unchecked")
	public SimpleEnumTypeHandlerBase() {
		this.clazz = (Class<E>) GenericUtils.getGenericParamClass(this.getClass(), SimpleEnumTypeHandlerBase.class, "E");
	}

	@Override
	public void setParameter(PreparedStatement ps, int i, E param, JdbcType jdbcType) throws SQLException {
		int sqlType = null == jdbcType ? this.sqlType() : jdbcType.ordinal();
		if (null == param) ps.setNull(i, sqlType);
		else ps.setObject(i, this.marshal(param), sqlType);
	}

	@Override
	public E getResult(ResultSet rs, String columnName) throws SQLException {
		return this.unmarshal(rs.getObject(columnName));
	}

	@Override
	public E getResult(CallableStatement cs, int columnIndex) throws SQLException {
		return this.unmarshal(cs.getObject(columnIndex));
	}

	// abstract protected Class<E> getEnumClass();

	@SuppressWarnings("unchecked")
	private E unmarshal(Object value) {
		if (null == value) return null;
		else {
			try {
				return (E) this.clazz.getMethod("parse", int.class).invoke(null, this.toInteger(value));
			} catch (Exception e1) {
				try {
					return (E) this.clazz.getMethod("parse", String.class).invoke(null, (String) value);
				} catch (Exception e2) {
					try {
						E[] values = (E[]) this.clazz.getClass().getMethod("getEnumConstants").invoke(this.clazz);
						return values[this.toInteger(value)];
					} catch (Exception e) {
						throw new RuntimeException("Invalid enum class generic.", e);
					}
				}
			}
		}
	}

	private int toInteger(Object value) {
		if (value.getClass().equals(String.class)) return Integer.parseInt(value.toString().trim());
		else if (Number.class.isAssignableFrom(value.getClass())) return ((Number) value).intValue();
		else throw new RuntimeException("Unknown type of enum value.");
	}

	private Object marshal(E value) {
		if (null == value) return null;
		try {
			Object r = value.getClass().getMethod("value").invoke(value);
			if (null != r && r.getClass() == Integer.class) r = r.toString();
			return r;
		} catch (Exception e) {
			return value.ordinal();
		}
	}
}
