package net.butfly.albacore.calculus.datasource;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;

import net.butfly.albacore.calculus.factor.Factor.Type;
import net.butfly.albacore.calculus.factor.Id;
import net.butfly.albacore.calculus.utils.Reflections;

public class ElasticDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 7474537351397729402L;
	public final String idField;

	protected ElasticDataDetail(Class<F> factor, String filter, String... url) {
		super(Type.ELASTIC, factor, filter, url);
		idField = findId(factor).getName();
	}

	@Override
	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		throw new UnsupportedOperationException("Elastic use EsPark to write, no output configuration need to be created.");
	}

	static Field findId(Class<?> c) {
		for (Field f : Reflections.getDeclaredFields(c))
			if (f.isAnnotationPresent(Id.class)) return f;
		return null;
	}
}
