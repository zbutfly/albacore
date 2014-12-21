package net.butfly.albacore.dbo.interceptor;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.RowBounds;

public abstract class PaginationInterceptor extends BaseStatementHandlerInterceptor {
	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		MetaObject meta = this.createMeta(invocation);
		if (this.getStatement(meta).getSqlCommandType() == SqlCommandType.SELECT) {
			RowBounds rowBounds = (RowBounds) meta.getValue("delegate.rowBounds");
			if (rowBounds != null && rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
				String boundSql = ((BoundSql) meta.getValue("delegate.boundSql")).getSql();
				String pageSql = this.paginate(boundSql, rowBounds.getOffset() - 1, rowBounds.getLimit());
				meta.setValue("delegate.boundSql.sql", pageSql);
				meta.setValue("delegate.rowBounds.offset", RowBounds.NO_ROW_OFFSET);
				meta.setValue("delegate.rowBounds.limit", RowBounds.NO_ROW_LIMIT);
				logger.trace("page sql generated: " + pageSql);
			}
		}
		return invocation.proceed();
	}

	abstract protected String paginate(String sql, int offset, int limit);
}
