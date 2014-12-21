package net.butfly.albacore.dbo.interceptor;

import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;

public abstract class BaseStatementHandlerInterceptor extends BaseInterceptor {
	protected Configuration getConfiguration(MetaObject meta) {
		return (Configuration) meta.getValue("delegate.configuration");
	}

	protected MappedStatement getStatement(MetaObject meta) {
		return (MappedStatement) meta.getValue("delegate.mappedStatement");
	}
}
