package net.butfly.albacore.dbo.dialect;

public class NoneDialect implements Dialect {
	@Override
	public String paginateWrap(String originalSql, int offset, int limit) {
		return originalSql;
	}
}
