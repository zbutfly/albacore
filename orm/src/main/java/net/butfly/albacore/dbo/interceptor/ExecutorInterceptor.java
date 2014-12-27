package net.butfly.albacore.dbo.interceptor;

import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.utils.ObjectUtils;
import net.butfly.albacore.utils.ReflectionUtils;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Intercepts({
		@Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class }),
		@Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
				ResultHandler.class, CacheKey.class, BoundSql.class }) })
public class ExecutorInterceptor extends BaseInterceptor {
	private static final Logger logger = LoggerFactory.getLogger(ExecutorInterceptor.class);
	private final Set<String> HACKED_STAT_ID_POOL = new HashSet<String>();
	private KeyGenerator keyGenerator;
	private String[] timeProps;

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		MappedStatement stat = (MappedStatement) invocation.getArgs()[0];
		if (stat.getSqlCommandType() == SqlCommandType.INSERT) {
			String sid = stat.getId();
			if (!HACKED_STAT_ID_POOL.contains(sid)) {
				HACKED_STAT_ID_POOL.add(sid);
				ReflectionUtils.safeFieldSet(stat, "keyGenerator", keyGenerator);
				logger.trace("KeyGenerator is hacked as [" + keyGenerator.getClass().getName() + "] for statement [" + sid
						+ "]");
			}
			MetaObject meta = ObjectUtils.createMeta(invocation.getArgs()[1]);
			for (String p : this.timeProps)
				if (meta.hasGetter(p) && meta.getValue(p) == null && meta.hasSetter(p)) {
					meta.setValue(p, new Date().getTime());
					logger.trace("Timestamp field [" + p + "] is filled.");
				}
		}
		if (null != invocation.getArgs()[1] && (invocation.getArgs()[1] instanceof Criteria))
			invocation.getArgs()[1] = ((Criteria) invocation.getArgs()[1]).getParameters();
		return invocation.proceed();
	}

	@Override
	public void setProperties(Properties properties) {
		String prop = properties.getProperty("timestampProps");
		this.timeProps = null == prop ? new String[0] : prop.split(",");
		try {
			this.keyGenerator = (KeyGenerator) Class.forName(properties.getProperty("keyGenerator")).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
