package net.butfly.albacore.dbo.interceptor;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.RowBounds;

public abstract class PaginationInterceptor extends AbstractInterceptor {
	private static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
	private static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();

	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		this.pagination((StatementHandler) invocation.getTarget());
		return invocation.proceed();
	}

	private void pagination(StatementHandler handler) {
		MetaObject meta = MetaObject.forObject(handler, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
		while (meta.hasGetter("h")) {
			Object object = meta.getValue("h");
			meta = MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
		}
		while (meta.hasGetter("target")) {
			Object object = meta.getValue("target");
			meta = MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
		}
		RowBounds rowBounds = (RowBounds) meta.getValue("delegate.rowBounds");
		if ((SqlCommandType) meta.getValue("delegate.mappedStatement.sqlCommandType") == SqlCommandType.SELECT
				&& rowBounds != null && rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
			String boundSql = (String) meta.getValue("delegate.boundSql.sql");
			meta.setValue("delegate.boundSql.sql", this.pagination(boundSql, rowBounds.getOffset() - 1, rowBounds.getLimit()));
			meta.setValue("delegate.rowBounds.offset", RowBounds.NO_ROW_OFFSET);
			meta.setValue("delegate.rowBounds.limit", RowBounds.NO_ROW_LIMIT);
			logger.trace("page sql generated: " + handler.getBoundSql().getSql());
		}
	}

	protected MappedStatement copyFromMappedStatement(MappedStatement ms, SqlSource newSqlSource) {
		MappedStatement.Builder builder = new MappedStatement.Builder(ms.getConfiguration(), ms.getId(), newSqlSource,
				ms.getSqlCommandType());
		builder.resource(ms.getResource());
		builder.fetchSize(ms.getFetchSize());
		builder.statementType(ms.getStatementType());
		builder.keyGenerator(ms.getKeyGenerator());
		if (ms.getKeyProperties() != null) {
			for (String keyProperty : ms.getKeyProperties()) {
				builder.keyProperty(keyProperty);
			}
		}
		builder.timeout(ms.getTimeout());
		builder.parameterMap(ms.getParameterMap());
		builder.resultMaps(ms.getResultMaps());
		builder.cache(ms.getCache());
		return builder.build();
	}

	abstract protected String pagination(String sql, int offset, int limit);
}
