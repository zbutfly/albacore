package net.butfly.albacore.dbo.interceptor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.dialect.Dialect;
import net.butfly.albacore.dbo.dialect.NoneDialect;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Intercepts({
		@Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class, CacheKey.class, BoundSql.class }),
		@Signature(type = Executor.class, method = "flushStatements", args = {}),
		@Signature(type = Executor.class, method = "commit", args = { boolean.class }),
		@Signature(type = Executor.class, method = "rollback", args = { boolean.class }),
		@Signature(type = Executor.class, method = "getTransaction", args = {}),
		@Signature(type = Executor.class, method = "close", args = { boolean.class }),
		@Signature(type = Executor.class, method = "isClosed", args = {}),
		@Signature(type = ParameterHandler.class, method = "getParameterObject", args = {}),
		@Signature(type = ParameterHandler.class, method = "setParameters", args = { PreparedStatement.class }),
		@Signature(type = ResultSetHandler.class, method = "handleResultSets", args = { Statement.class }),
		@Signature(type = ResultSetHandler.class, method = "handleOutputParameters", args = { CallableStatement.class }),
		@Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class }),
		@Signature(type = StatementHandler.class, method = "parameterize", args = { Statement.class }),
		@Signature(type = StatementHandler.class, method = "batch", args = { Statement.class }),
		@Signature(type = StatementHandler.class, method = "update", args = { Statement.class }),
		@Signature(type = StatementHandler.class, method = "query", args = { Statement.class, ResultHandler.class }) })
public class CoreInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(CoreInterceptor.class);
	private String[] timeProps;
	private Dialect dialect;

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		logger.error(invocation.getTarget().getClass().getSimpleName()+"."+invocation.getMethod().getName());
		return invocation.proceed();
	}

	protected void doIntercept(Invocation invocation) {
//		MetaObject meta = ObjectUtils.createMeta(invocation.getTarget());
//		SqlCommandType type = this.getStatement(meta).getSqlCommandType();
//		Object paramsObject = meta.getValue("delegate.parameterHandler.parameterObject");
//		if (type == SqlCommandType.SELECT) {
//			RowBounds rowBounds = (RowBounds) meta.getValue("delegate.rowBounds");
//			if (rowBounds != null && rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
//				String boundSql = ((BoundSql) meta.getValue("delegate.boundSql")).getSql();
//				if (type == SqlCommandType.SELECT && null != paramsObject && paramsObject instanceof Criteria)
//					boundSql = this.appendOrderBy(boundSql, (Criteria) paramsObject);
//				boundSql = this.paginate(boundSql, rowBounds.getOffset() - 1, rowBounds.getLimit());
//				meta.setValue("delegate.boundSql.sql", boundSql);
//				meta.setValue("delegate.rowBounds.offset", RowBounds.NO_ROW_OFFSET);
//				meta.setValue("delegate.rowBounds.limit", RowBounds.NO_ROW_LIMIT);
//				logger.trace("page sql generated: " + boundSql);
//			}
//		}
//		// fill timestamp props.
//		if (type == SqlCommandType.INSERT) {
//			MetaObject paramsMeta = ObjectUtils.createMeta(paramsObject);
//			for (String p : this.timeProps)
//				if (paramsMeta.hasGetter(p) && paramsMeta.getValue(p) == null && paramsMeta.hasSetter(p))
//					paramsMeta.setValue(p, new Date().getTime());
//		}
//		// unwrap criteria
//		if (null != paramsObject && paramsObject instanceof Criteria) paramsObject = ((Criteria) paramsObject).getParameters();
//		meta.setValue("delegate.parameterHandler.parameterObject", paramsObject);

	}

	protected String appendOrderBy(String boundSql, Criteria criteria) {
		String orderBy = criteria.getOrderBy();
		return orderBy == null ? boundSql : boundSql + orderBy;
	}

	@Override
	public void setProperties(Properties properties) {
		String prop = properties.getProperty("timestampProps");
		this.timeProps = null == prop ? new String[0] : prop.split(",");
		prop = properties.getProperty("dialect");
		if (null == prop) this.dialect = new NoneDialect();
		else try {
			this.dialect = (Dialect) Class.forName(properties.getProperty("dialect")).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}
}
