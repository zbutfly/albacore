package net.butfly.albacore.dbo.interceptor;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

@Intercepts({ @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class,
		RowBounds.class, ResultHandler.class }) })
public class PaginationOracleInterceptor extends PaginationInterceptor {
	private final static String PAGE_PARTTERN = "select * from (select TMP_A__.*, rownum row_num__ from (%s) TMP_A__ ) TMP_B__ where TMP_B__.ROW_NUM__ between %s and %s";

	@Override
	protected String pagination(String sql, int offset, int limit) {
		return String.format(PAGE_PARTTERN, sql, offset + 1, offset + limit);
	}
}