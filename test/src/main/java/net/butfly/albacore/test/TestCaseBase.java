package net.butfly.albacore.test;

import java.io.IOException;
import java.sql.Driver;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

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
		putDataSourceIntoJNDI(
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

	public final static void putDataSourceIntoJNDI(String jndiName, Object ds) {
		System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
		System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");
		InitialContext ic;
		try {
			ic = new InitialContext();
			ic.createSubcontext("java:");
			ic.createSubcontext("java:/comp");
			ic.createSubcontext("java:/comp/env");
			ic.createSubcontext("java:/comp/env/jdbc");
			ic.bind("java:/comp/env/" + jndiName, ds);
		} catch (NamingException e) {
			throw new RuntimeException(e);
		}
	}
}
