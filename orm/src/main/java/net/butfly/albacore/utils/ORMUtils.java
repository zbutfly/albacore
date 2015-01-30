package net.butfly.albacore.utils;

import java.sql.Driver;

import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public final class ORMUtils extends UtilsBase {
	private ORMUtils() {}

	@SuppressWarnings("unchecked")
	static public AbstractDataSource createDatasource(String driverClass, String url, String username, String password) {
		SimpleDriverDataSource ds = new SimpleDriverDataSource();
		ds.setDriverClass((Class<? extends Driver>) Reflections.forClassName(driverClass));
		ds.setUrl(url);
		ds.setUsername(username);
		ds.setPassword(password);
		return ds;
	}
}
