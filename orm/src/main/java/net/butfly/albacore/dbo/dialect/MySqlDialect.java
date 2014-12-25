package net.butfly.albacore.dbo.dialect;

public class MySqlDialect implements Dialect {
	@Override
	public String paginateWrap(String originalSql, int offset, int limit) {
		return originalSql + " limit " + offset + " ," + limit;
	}
}
