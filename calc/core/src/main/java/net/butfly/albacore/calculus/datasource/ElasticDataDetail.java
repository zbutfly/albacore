package net.butfly.albacore.calculus.datasource;

import org.apache.hadoop.conf.Configuration;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class ElasticDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 7474537351397729402L;

	protected ElasticDataDetail(Class<F> factor, String source, String filter, String... url) {
		// TODO: key class?
		super(Type.ELASTIC, factor, source, filter, url);
	}

	@Override
	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		throw new UnsupportedOperationException("Elastic use EsPark to write, no output configuration need to be created.");
	}
}
