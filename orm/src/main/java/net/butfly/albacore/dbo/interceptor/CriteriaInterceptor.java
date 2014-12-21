package net.butfly.albacore.dbo.interceptor;

import net.butfly.albacore.dbo.criteria.Criteria;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

@Intercepts({
		@Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class, CacheKey.class, BoundSql.class }) })
public class CriteriaInterceptor extends BaseExecutorInterceptor {
	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		Object params = invocation.getArgs()[1];
		if (null != params && params instanceof Criteria) invocation.getArgs()[1] = ((Criteria) params).getParameters();
		return invocation.proceed();
	}
}
