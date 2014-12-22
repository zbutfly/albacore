package net.butfly.albacore.dbo.dialect;

public class OracleDialect implements Dialect {
	private final static String PAGE_PARTTERN = "select * from (select TMP_A__.*, rownum row_num__ from (%s) TMP_A__ ) TMP_B__ where TMP_B__.ROW_NUM__ between %s and %s";

	@Override
	public String paginateWrap(String originalSql, int offset, int limit) {
		return String.format(PAGE_PARTTERN, originalSql, offset + 1, offset + limit);
	}
}
