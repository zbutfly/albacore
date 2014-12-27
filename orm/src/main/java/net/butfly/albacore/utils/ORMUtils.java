package net.butfly.albacore.utils;

import java.sql.Driver;

import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public final class ORMUtils extends UtilsBase {
	private ORMUtils() {}

	@SuppressWarnings("unchecked")
	static public AbstractDataSource createDatasource(String driverClass, String url, String username, String password) {
		SimpleDriverDataSource ds = new SimpleDriverDataSource();
		try {
			ds.setDriverClass((Class<? extends Driver>) Class.forName(driverClass));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		ds.setUrl(url);
		ds.setUsername(username);
		ds.setPassword(password);
		return ds;
	}
}
