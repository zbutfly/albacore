package net.butfly.albacore.utils;

import java.sql.Driver;

import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public final class ORMUtils extends Utils {
	private ORMUtils() {}

	static public AbstractDataSource createDatasource(String driverClass, String url, String username, String password) {
		SimpleDriverDataSource ds = new SimpleDriverDataSource();
		Class<? extends Driver> cl = Reflections.forClassName(driverClass);
		ds.setDriverClass(cl);
		ds.setUrl(url);
		ds.setUsername(username);
		ds.setPassword(password);
		return ds;
	}
}
