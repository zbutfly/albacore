package net.butfly.albacore.dbo.interceptor;

import java.sql.Connection;
import java.util.Properties;

import net.butfly.albacore.dbo.dialect.Dialect;
import net.butfly.albacore.dbo.interceptor.ExecutorInterceptor.RowBoundsWrapper;
import net.butfly.albacore.utils.ObjectUtils;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * order:
 * 1. Executor.update/Executor.query
 * 2. StatementHandler.prepare
 * 3. StatementHandler.parameterize
 * 4. ParameterHandler.setParameters
 * 5. StatementHandler.batch/StatementHandler.query
 * 6. ResultSetHandler.handleResultSets
 * 7. Executor.commit
 * 8. Executor.close
 * 
 * @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }), 	
 * @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class }),
 * @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class }),
 * @Signature(type = Executor.class, method = "flushStatements", args = {}),
 * @Signature(type = Executor.class, method = "commit", args = { boolean.class }),
 * @Signature(type = Executor.class, method = "rollback", args = { boolean.class }),
 * @Signature(type = Executor.class, method = "getTransaction", args = {}),
 * @Signature(type = Executor.class, method = "close", args = { boolean.class }),
 * @Signature(type = Executor.class, method = "isClosed", args = {}),
 * @Signature(type = ParameterHandler.class, method = "getParameterObject", args = {}),
 * @Signature(type = ParameterHandler.class, method = "setParameters", args = { PreparedStatement.class }),
 * @Signature(type = StatementHandler.class, method = "prepare", args = { Connection.class }),
 * @Signature(type = StatementHandler.class, method = "parameterize", args = { Statement.class }),
 * @Signature(type = StatementHandler.class, method = "batch", args = { Statement.class }),
 * @Signature(type = StatementHandler.class, method = "update", args = { Statement.class }),
 * @Signature(type = StatementHandler.class, method = "query", args = { Statement.class, ResultHandler.class }),
 * @Signature(type = ResultSetHandler.class, method = "handleResultSets", args = { Statement.class }),
 * @Signature(type = ResultSetHandler.class, method = "handleOutputParameters", args = { CallableStatement.class }),
 * 
 * things: 
 * append order // select param/sql
 * wrap pagination // select sql
 * unwrap criteria // select param -- Executor.query
 * 
 * fill timestamp // insert param
 * </pre>
 * 
 * @author butfly
 *
 */
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
				if (rowBounds instanceof RowBoundsWrapper) {
					sql = sql + ((RowBoundsWrapper) rowBounds).orderBy;
					rowBounds = ((RowBoundsWrapper) rowBounds).unwrap();
					meta.setValue("delegate.rowBounds", rowBounds);
					logger.trace("OrderBy is appendded on SQL: " + sql.replaceAll("[\\n]", "").replaceAll("[ \t]+", " "));
				}
				// wrap pagination
				if (rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
					sql = this.dialect.paginateWrap(sql, rowBounds.getOffset() - 1, rowBounds.getLimit());
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
