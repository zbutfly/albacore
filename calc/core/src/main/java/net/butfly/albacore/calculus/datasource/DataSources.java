package net.butfly.albacore.calculus.datasource;

import java.util.HashMap;

import net.butfly.albacore.calculus.factor.Factor;

public class DataSources extends HashMap<String, DataSource<?, ?>> {
	private static final long serialVersionUID = -7809799411800022817L;

	@SuppressWarnings("unchecked")
	public <K, F extends Factor<F>> DataSource<K, F> get(String dbid, Class<K> keyClass, Class<F> factorClass) {
		return (DataSource<K, F>) super.get(dbid);
	}
}
