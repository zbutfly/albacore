package net.butfly.albacore.dbo.interceptor;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import net.butfly.albacore.utils.ReflectionUtils;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;

@Intercepts({ @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }) })
public class KeyGeneratorInterceptor extends BaseExecutorInterceptor {
	private final Set<String> altered = new HashSet<String>();
	private KeyGenerator keyGenerator;

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		MappedStatement stat = (MappedStatement) invocation.getArgs()[0];
		String statId = stat.getId();
		if (!altered.contains(statId)) {
			altered.add(statId);
			if (stat.getSqlCommandType() == SqlCommandType.INSERT)
				ReflectionUtils.safeFieldSet(MappedStatement.class.getDeclaredField("keyGenerator"), stat, keyGenerator);
		}
		return invocation.proceed();
	}

	@Override
	public void setProperties(Properties properties) {
		try {
			this.keyGenerator = (KeyGenerator) Class.forName(properties.getProperty("keyGeneratorClass")).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
