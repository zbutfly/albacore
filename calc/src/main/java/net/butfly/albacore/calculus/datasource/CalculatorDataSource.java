package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Connection;
import org.jongo.Jongo;

import com.google.common.base.Joiner;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.Functor.Type;
import net.butfly.albacore.calculus.marshall.HbaseResultMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;

public abstract class CalculatorDataSource implements Serializable {
	private static final long serialVersionUID = 1L;
	private Functor.Type type;
	private Marshaller<?, ?> marshaller;

	public Functor.Type getType() {
		return type;
	}

	public Marshaller<?, ?> getMarshaller() {
		return marshaller;
	}

	public CalculatorDataSource(Type type, Marshaller<?, ?> marshaller) {
		super();
		this.type = type;
		this.marshaller = marshaller;
	}

	@Override
	public String toString() {
		return "CalculatorDataSource:" + this.type;
	}

	public static class KafkaDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = 7500441385655250814L;
		private String quonum;
		private String group;

		public KafkaDataSource(String quonum, String group) {
			super(Type.KAFKA, new KafkaMarshaller());
			this.quonum = quonum;
			this.group = group;
		}

		@Override
		public String toString() {
			return super.toString() + ":" + this.quonum + "(" + group + ")";
		}

		public String getQuonum() {
			return quonum;
		}

		public String getGroup() {
			return group;
		}
	}

	public static class HbaseDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = 3367501286179801635L;
		private String configFile;
		private Connection hconn;

		public HbaseDataSource(String configFile) {
			super(Type.HBASE, new HbaseResultMarshaller());
			this.configFile = configFile;
			// XXX
			// this.hconn = ConnectionFactory.createConnection(conf)
		}

		@Override
		public String toString() {
			return super.toString() + ":" + this.configFile;
		}

		public String getConfigFile() {
			return configFile;
		}

		public Connection getHconn() {
			return hconn;
		}
	}

	public static class MongoDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = -2617369621178264387L;
		private String uri;
		private String authuri;

		private MongoClient client;
		private String db;
		private DB mongo;
		private Jongo jongo;

		@SuppressWarnings("deprecation")
		public MongoDataSource(String uri, String authuri) {
			super(Type.MONGODB, new MongoMarshaller());
			this.uri = uri;
			this.authuri = authuri;

			MongoClientURI u = new MongoClientURI(this.uri);
			this.client = new MongoClient(u);
			this.db = u.getDatabase();
			this.mongo = this.client.getDB(db);
			this.jongo = new Jongo(this.mongo);
		}

		@Override
		public String toString() {
			return super.toString() + ":" + this.uri + "." + db;
		}

		public String getUri() {
			return uri;
		}

		public String getAuthuri() {
			return authuri;
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

		public String getDb() {
			return db;
		}
	}

	public static class ConstDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = -673387208224779163L;
		private String[] values;

		public ConstDataSource(String[] values) {
			super(Type.CONSTAND_TO_CONSOLE, null);
			this.values = values;
		}

		@Override
		public String toString() {
			return super.toString() + ":" + Joiner.on(',').join(values);
		}

		public String[] getValues() {
			return values;
		}
	}
}
