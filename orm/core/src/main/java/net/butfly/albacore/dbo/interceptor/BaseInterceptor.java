package net.butfly.albacore.dbo.interceptor;

import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
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
public abstract class BaseInterceptor implements Interceptor {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final MetaObject createMeta(Invocation invocation) {
		return Objects.createMeta(invocation.getTarget());
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}
}
