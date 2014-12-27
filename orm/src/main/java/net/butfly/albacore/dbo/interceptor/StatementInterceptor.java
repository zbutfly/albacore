package net.butfly.albacore.dbo.interceptor;

import java.sql.Connection;
import java.util.Properties;

import net.butfly.albacore.dbo.criteria.CriteriaMap;
import net.butfly.albacore.dbo.criteria.CriteriaMap.QueryType;
import net.butfly.albacore.dbo.dialect.Dialect;
import net.butfly.albacore.utils.ObjectUtils;
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
	private Dialect dialect;

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		MetaObject meta = ObjectUtils.createMeta(invocation.getTarget());
		// misc hacking
		if (((MappedStatement) meta.getValue("delegate.mappedStatement")).getSqlCommandType() == SqlCommandType.SELECT) {
			RowBounds rowBounds = (RowBounds) meta.getValue("delegate.rowBounds");
			String sql = (String) meta.getValue("delegate.boundSql.sql");
			if (null != rowBounds) {
				// append order
				Object paramsObject = meta.getValue("delegate.boundSql.parameterObject");
				if (paramsObject instanceof CriteriaMap) {
					CriteriaMap params = (CriteriaMap) paramsObject;
					QueryType type = params.getType();
					String orderBy = params.getOrderBy();
					if (null != params && QueryType.LIST == type && null != orderBy) {
						sql = sql + orderBy;
						logger.trace("OrderBy is appendded on SQL: " + sql.replaceAll("[\\n]", "").replaceAll("[ \t]+", " "));
					}
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
			this.dialect = (Dialect) Class.forName(properties.getProperty("dialect")).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
