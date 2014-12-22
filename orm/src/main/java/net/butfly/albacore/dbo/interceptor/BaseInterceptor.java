package net.butfly.albacore.dbo.interceptor;

import net.butfly.albacore.utils.ObjectUtils;

import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.reflection.MetaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseInterceptor implements Interceptor {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final MetaObject createMeta(Invocation invocation) {
		return ObjectUtils.createMeta(invocation.getTarget());
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}
}
