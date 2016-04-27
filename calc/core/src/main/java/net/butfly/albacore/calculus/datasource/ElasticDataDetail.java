package net.butfly.albacore.calculus.datasource;

import org.apache.hadoop.conf.Configuration;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class ElasticDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 7474537351397729402L;

	protected ElasticDataDetail(Class<F> factor, String filter, String... url) {
		super(Type.ELASTIC, factor, filter, url);
	}

	@Override
	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		throw new UnsupportedOperationException("Elastic use EsPark to write, no output configuration need to be created.");
	}
}
