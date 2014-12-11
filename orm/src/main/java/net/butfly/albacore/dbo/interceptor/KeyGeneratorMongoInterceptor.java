package net.butfly.albacore.dbo.interceptor;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Signature;
import org.bson.types.ObjectId;

@Intercepts({ @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }) })
public class KeyGeneratorMongoInterceptor extends KeyGeneratorInterceptor<ObjectId> {
	@Override
	protected ObjectId generatorKey() {
		return new ObjectId();
	}
}
