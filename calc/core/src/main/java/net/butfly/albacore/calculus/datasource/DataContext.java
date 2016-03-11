package net.butfly.albacore.calculus.datasource;

import org.jongo.Jongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.calculus.datasource.DataSource.MongoDataSource;

public abstract class DataContext {
	DataSource datasource;

	public DataContext(DataSource ds) {
		this.datasource = ds;
	}

	public static class MongoContext extends DataContext {
		MongoClientURI uri;
		MongoClient client;
		DB mongo;
		Jongo jongo;

		@SuppressWarnings("deprecation")
		public MongoContext(DataSource ds) {
			super(ds);
			MongoDataSource mds = (MongoDataSource) ds;
			this.uri = new MongoClientURI(mds.uri);
			this.client = new MongoClient(this.uri);
			this.mongo = this.client.getDB(this.uri.getDatabase());
			this.jongo = new Jongo(this.mongo);
		}

		public MongoClient getClient() {
			return client;
		}

		public DB getMongo() {
			return mongo;
		}

		public Jongo getJongo() {
			return jongo;
		}
	}
}
