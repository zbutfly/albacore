package net.butfly.albacore.dbo.interceptor;

import java.sql.Connection;
import java.util.Properties;

import net.butfly.albacore.dbo.criteria.OrderedRowBounds;
import net.butfly.albacore.dbo.dialect.Dialect;
import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Intercepts({ @Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class }) })
public class StatementInterceptor extends BaseInterceptor {
	private static final Logger logger = LoggerFactory.getLogger(StatementInterceptor.class);
	private Dialect dialect = null;

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		MetaObject meta = Objects.createMeta(invocation.getTarget());
		// misc hacking
		if (((MappedStatement) meta.getValue("delegate.mappedStatement")).getSqlCommandType() == SqlCommandType.SELECT) {
			String sql = (String) meta.getValue("delegate.boundSql.sql");
			// logger.trace("Statement preparing intercepted with orininal sql:
			// \n\t" + sql);
			RowBounds rowBounds = (RowBounds) meta.getValue("delegate.rowBounds");

			if (null != rowBounds && null != this.dialect) {
				// append order
				if (rowBounds instanceof OrderedRowBounds) {
					sql = this.dialect.orderByWrap(sql, ((OrderedRowBounds) rowBounds).getOrderFields());
					logger.trace("OrderBy is appendded on SQL: " + sql.replaceAll("[\\n]", "").replaceAll("[ \t]+", " "));
				}
				// wrap pagination
				if (rowBounds.getLimit() != RowBounds.NO_ROW_LIMIT) {
					sql = this.dialect.paginateWrap(sql, rowBounds.getOffset(), rowBounds.getLimit());
					logger.trace("Pagination is wrapped on SQL: " + sql.replaceAll("[\\n]", "").replaceAll("[ \t]+", " "));
				}
				meta.setValue("delegate.boundSql.sql", sql);
				meta.setValue("delegate.rowBounds.offset", RowBounds.NO_ROW_OFFSET);
				meta.setValue("delegate.rowBounds.limit", RowBounds.NO_ROW_LIMIT);
			}
		}

		return invocation.proceed();
	}

	@Override
	public void setProperties(Properties properties) {
		try {
			String dial = properties.getProperty("dialect");
			if (null != dial) this.dialect = Reflections.construct(properties.getProperty("dialect"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
