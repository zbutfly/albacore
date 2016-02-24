package net.butfly.albacore.dbo.dialect;

import net.butfly.albacore.dbo.criteria.OrderField;

public interface Dialect {
	String paginateWrap(String originalSql, int offset, int limit);

	String orderByWrap(String originalSql, OrderField[] fields);
}
