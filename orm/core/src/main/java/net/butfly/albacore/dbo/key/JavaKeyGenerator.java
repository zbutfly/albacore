package net.butfly.albacore.dbo.key;

import java.io.Serializable;
import java.sql.Statement;

import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JavaKeyGenerator<K extends Serializable> implements KeyGenerator {
	private static final String[] DEFAULT_KEY_PROPERTIES = new String[] { "id" };
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	protected abstract K generateKey();

	@Override
	public void processBefore(Executor executor, MappedStatement ms, Statement stmt, Object parameter) {
		if (parameter == null) return;
		MetaObject meta = Objects.createMeta(parameter);
		String[] ids = ms.getKeyProperties();
		for (String id : ids == null ? DEFAULT_KEY_PROPERTIES : ids)
			if (meta.hasGetter(id) && meta.getValue(id) == null && meta.hasSetter(id)) meta.setValue(id, this.generateKey());
	}

	@Override
	public void processAfter(Executor executor, MappedStatement ms, Statement stmt, Object parameter) {}
}
