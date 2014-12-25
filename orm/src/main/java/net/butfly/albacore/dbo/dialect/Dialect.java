package net.butfly.albacore.dbo.dialect;

public interface Dialect {
	String paginateWrap(String originalSql, int offset, int limit);
}
