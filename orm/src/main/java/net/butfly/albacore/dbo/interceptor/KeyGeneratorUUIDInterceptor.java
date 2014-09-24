package net.butfly.albacore.dbo.interceptor;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Signature;

import net.butfly.albacore.dbo.type.GUID;

@Intercepts({ @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }) })
public class KeyGeneratorUUIDInterceptor extends KeyGeneratorInterceptor<String> {
	@Override
	protected String generatorKey() {
		return GUID.randomGUID().toString().toUpperCase();
	}
}
