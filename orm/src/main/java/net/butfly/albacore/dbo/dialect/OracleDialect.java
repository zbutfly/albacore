package net.butfly.albacore.dbo.dialect;

import net.butfly.albacore.dbo.criteria.OrderField;

public class OracleDialect implements Dialect {
	private static final String PAGE_PARTTERN = "select * from (select TMP_A__.*, rownum row_num__ from (%s) TMP_A__ ) TMP_B__ where TMP_B__.ROW_NUM__ between %s and %s";

	@Override
	public String paginateWrap(String originalSql, int offset, int limit) {
		return String.format(PAGE_PARTTERN, originalSql, offset + 1, offset + limit);
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
