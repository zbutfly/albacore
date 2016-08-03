package net.butfly.albacore.calculus.factor.detail;

import org.apache.hadoop.conf.Configuration;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.Type;
import net.butfly.albacore.calculus.factor.FactroingConfig;

public class ElasticFractoring<F> extends FactroingConfig<F> {
	private static final long serialVersionUID = 7474537351397729402L;

	protected ElasticFractoring(Class<F> factor, String source, String url, String filter) {
		super(Type.ELASTIC, factor, source, url, filter);
	}

	@Override
	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		throw new UnsupportedOperationException("Elastic use EsPark to write, no output configuration need to be created.");
	}
}
