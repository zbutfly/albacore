package net.butfly.albacore.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.butfly.albacore.utils.logger.Logger;

public abstract class SpringCase {
	private static Logger slogger = Logger.getLogger(SpringCase.class);
	protected Logger logger = Logger.getLogger(this.getClass());

	private static Map<String, DataSource> allDS = null;
	private ApplicationContext context;
	protected DataSource[] dataSources;

	private static boolean initialized = false;

	abstract protected String[] getConfiguration();

	abstract protected void initialize();

	@BeforeClass
	public static void setUpBeforeClass() throws NamingException {
		slogger.debug("JUnit BeforeClass entered.");
	}

	@AfterClass
	public static void tearDownAfterClass() {
		slogger.debug("JUnit AfterClass entered.");
	}

	@Before
	public void setUp() throws NamingException {
		logger.debug("JUnit Before entered.");
		if (!initialized) {
			initialized = true;
			dataSources = jndi();
			String[] conf = this.getConfiguration();
			if (null != conf && conf.length > 0) {
				this.context = new ClassPathXmlApplicationContext(this.getConfiguration());
			} else {
				this.context = null;
			}
			this.initialize();
		}
	}

	@After
	public void tearDown() {
		logger.debug("JUnit After entered.");
	}

	@SuppressWarnings("unchecked")
	protected <T> T getBean(String name) {
		return null == this.context ? null : (T) this.context.getBean(name);
	}

	protected <T> T getBean(Class<T> clazz) {
		return null == this.context ? null : this.context.getBean(clazz);
	}

	private static final String JNDI_JDBC_PREFIX = "java:comp/env/jdbc";

	private static DataSource[] jndi() throws NamingException {
		if (allDS == null) {
			allDS = new HashMap<>();
			Context root = new InitialContext();
			for (Binding b : Collections.list(root.listBindings(JNDI_JDBC_PREFIX))) {
				if (b.getObject() instanceof DataSource) {
					Logger.getLogger(b.getObject().getClass()).info("DataSource found: [" + JNDI_JDBC_PREFIX + "/" + b.getName() + "]:[" + b
							.getObject().toString() + "]");
					allDS.put(b.getName(), (DataSource) b.getObject());
				}
			}
		}
		return allDS.values().toArray(new DataSource[allDS.size()]);
	}
}
