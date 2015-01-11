package net.butfly.albacore.dbo.dialect;

import net.butfly.albacore.dbo.criteria.OrderField;

public class MySqlDialect implements Dialect {
	@Override
	public String paginateWrap(String originalSql, int offset, int limit) {
		return originalSql + " limit " + offset + " ," + limit;
	}

	@Override
	public String orderByWrap(String originalSql, OrderField[] fields) {
		if (null == fields || fields.length == 0) return originalSql;
		StringBuilder sb = new StringBuilder(originalSql).append(" ORDER BY");
		for (OrderField o : fields)
			sb.append(" ").append(o.toString());
		return sb.toString();
	}
}
