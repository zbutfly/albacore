package net.butfly.albacore.dbo.handler;

import java.lang.reflect.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.butfly.albacore.support.EnumSupport;
import net.butfly.albacore.utils.EnumUtils;

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

public abstract class EnumTypeSetHandlerBase<E extends EnumSupport<E>> implements TypeHandler<E[]> {
	private static String SEPERATOR = "/";

	@Override
	public void setParameter(PreparedStatement ps, int i, E[] parameter, JdbcType jdbcType) throws SQLException {
		ps.setString(i, parameter == null ? this.join(EnumUtils.values(this.getEnumClass())) : this.join(parameter));
	}

	@Override
	public E[] getResult(ResultSet rs, String columnName) throws SQLException {
		String value = rs.getString(columnName);
		return null == value ? null : this.splite(value);
	}

	@Override
	public E[] getResult(CallableStatement cs, int columnIndex) throws SQLException {
		String value = cs.getString(columnIndex);
		return null == value ? null : this.splite(value);
	}

	@SuppressWarnings("unchecked")
	private E[] splite(String prices) {
		if (prices == null || "".equals(prices)) { return (E[]) Array.newInstance(this.getEnumClass(), 0); }
		String[] pricesTypes = prices.split(SEPERATOR);
		E[] list = (E[]) Array.newInstance(this.getEnumClass(), pricesTypes.length);
		for (int i = 0; i < pricesTypes.length; i++) {
			String str = pricesTypes[i];
			list[i] = (EnumUtils.valueOf(this.getEnumClass(), Integer.parseInt(str)));
		}
		return list;
	}

	private String join(E[] list) {
		StringBuilder sb = new StringBuilder();
		for (E pt : list) {
			sb.append(pt.value()).append(SEPERATOR);
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	abstract protected Class<E> getEnumClass();
}
