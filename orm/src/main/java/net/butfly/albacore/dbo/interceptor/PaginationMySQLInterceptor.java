package net.butfly.albacore.dbo.interceptor;

import java.sql.Connection;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Signature;

@Intercepts({ @Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class }) })
public class PaginationMySQLInterceptor extends PaginationInterceptor {
	@Override
	protected String paginate(String sql, int offset, int limit) {
		return sql.replaceAll("[\r\n]", " ").replaceAll("\\s{2,}", " ").replaceAll("[^\\s,]+\\.", "") + " limit " + offset
				+ " ," + limit;
	}
}
