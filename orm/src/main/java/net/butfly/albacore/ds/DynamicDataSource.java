package net.butfly.albacore.ds;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class DynamicDataSource extends AbstractRoutingDataSource {
	public static class DatabaseContextHolder {
		private static final ThreadLocal<String> contextHolder = new ThreadLocal<String>();

		public static final String DB2 = "db2";
		public static final String SQLSERVER = "sqlserver";
		public static final String SQLSERVER2 = "sqlserver2";

		private DatabaseContextHolder() {}

		public static void set(String dbType) {
			contextHolder.set(dbType);
		}

		public static String get() {
			return contextHolder.get();
		}

		public static void clear() {
			contextHolder.remove();
		}
	}

	protected Object determineCurrentLookupKey() {
		return DatabaseContextHolder.get();
	}

	@Override
	public java.util.logging.Logger getParentLogger() {
		// Logger logger = LoggerFactory.getLogger(DynamicDataSource.class);
		return null;
	}
}
