package net.butfly.albacore.dbo.interceptor;

import java.lang.reflect.Method;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

public abstract class BaseExecutorInterceptor extends BaseInterceptor {
	protected enum ExecutorMethod {
		UPDATE, QUERY, QUERY_CACHE;
		private Method method;

		private static ExecutorMethod check(Invocation invocation) {
			for (ExecutorMethod m : ExecutorMethod.values())
				if (invocation.getMethod().equals(m.method)) return m;
			throw new RuntimeException();
		}
	}

	static {
		try {
			ExecutorMethod.UPDATE.method = Executor.class.getMethod("update", MappedStatement.class, Object.class);
			ExecutorMethod.QUERY.method = Executor.class.getMethod("query", MappedStatement.class, Object.class,
					RowBounds.class, ResultHandler.class);
			ExecutorMethod.QUERY_CACHE.method = Executor.class.getMethod("query", MappedStatement.class, Object.class,
					RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected ExecutorMethod check(Invocation invocation) {
		return ExecutorMethod.check(invocation);
	}

	protected RowBounds getRowBounds(Invocation invocation) {
		if (this.check(invocation) == ExecutorMethod.UPDATE) throw new UnsupportedOperationException();
		return (RowBounds) invocation.getArgs()[2];
	}
}
