package net.butfly.albacore.dbo.interceptor;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

@Intercepts({
		@Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class }) })
public class CriteriaInterceptor<K extends Serializable> extends AbstractInterceptor {
	public Object intercept(Invocation invocation) throws Throwable {
		Object[] queryArgs = invocation.getArgs();
		if (queryArgs[1] instanceof Criteria) queryArgs[1] = ((Criteria) queryArgs[1]).getParameters();
		return invocation.proceed();
	}
}
