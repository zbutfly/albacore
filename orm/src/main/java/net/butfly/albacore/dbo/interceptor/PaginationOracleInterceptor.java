package net.butfly.albacore.dbo.interceptor;

import java.sql.Connection;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Signature;

@Intercepts({ @Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class }) })
public class PaginationOracleInterceptor extends PaginationInterceptor {
	private final static String PAGE_PARTTERN = "select * from (select TMP_A__.*, rownum row_num__ from (%s) TMP_A__ ) TMP_B__ where TMP_B__.ROW_NUM__ between %s and %s";

	@Override
	protected String paginate(String sql, int offset, int limit) {
		return String.format(PAGE_PARTTERN, sql, offset + 1, offset + limit);
	}
}