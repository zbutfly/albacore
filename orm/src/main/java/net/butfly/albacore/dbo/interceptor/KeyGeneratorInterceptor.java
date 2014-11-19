package net.butfly.albacore.dbo.interceptor;

import java.io.Serializable;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;

@Intercepts({ @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }) })
public abstract class KeyGeneratorInterceptor<K extends Serializable> extends AbstractInterceptor {
	@Override
	public final Object intercept(Invocation invocation) throws Throwable {
		Object[] queryArgs = invocation.getArgs();
		MappedStatement ms = (MappedStatement) queryArgs[0];
		if (ms.getSqlCommandType() == SqlCommandType.INSERT) {
			Configuration conf = ms.getConfiguration();
			final MetaObject meta = conf.newMetaObject(queryArgs[1]);
			String[] keys = ms.getKeyProperties();
			if (null != keys) for (String key : keys) {
				if (null != key && meta.hasSetter(key) && null == meta.getValue(key)) {
					meta.setValue(key, this.generatorKey());
					queryArgs[1] = meta.getOriginalObject();
				}
			}
		}
		return invocation.proceed();
	}

	protected abstract K generatorKey();
}
