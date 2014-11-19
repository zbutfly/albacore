package net.butfly.albacore.test;

import java.io.IOException;
import java.sql.Driver;
import java.util.Properties;

import net.butfly.albacore.utils.JNDIUtils;

import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public abstract class TestCaseBase extends SpringTestCaseBase {
	@SuppressWarnings("unchecked")
	private AbstractDataSource createDatasource(String driverClass, String url, String username, String password) {
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

	protected void initSpringContext() {
		Properties prop = this.loadProperties();
		JNDIUtils.putDataSourceIntoJNDI(
				prop.getProperty("jdbc.jndiname"),
				createDatasource(prop.getProperty("jdbc.drivername"), prop.getProperty("jdbc.url"),
						prop.getProperty("jdbc.username"), prop.getProperty("jdbc.password")));
		super.initSpringContext();
	}

	protected Properties loadProperties() {
		Properties prop = new Properties();
		try {
			prop.load(this.getClass().getResourceAsStream("/jdbc.properties"));
			prop.load(this.getClass().getResourceAsStream("/jdbc-test.properties"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return prop;
	}
}
