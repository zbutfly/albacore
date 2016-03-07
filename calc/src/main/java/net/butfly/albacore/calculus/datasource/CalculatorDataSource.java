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
		private String servers;
		private String root;
		private String group;
		private int topicPartitions;

		public KafkaDataSource(String servers, String root, int topicPartitions, String group) {
			super(Type.KAFKA, new KafkaMarshaller());
			int pos = servers.indexOf('/');
			if (root == null && pos >= 0) {
				this.servers = servers.substring(0, pos);
				this.root = servers.substring(pos + 1);
			} else {
				this.root = root;
				this.servers = servers;
			}
			this.group = group;
			this.topicPartitions = topicPartitions;
		}

		@Override
		public String toString() {
			return super.toString() + ":" + this.getServers() + "(" + group + ")";
		}

		public String getGroup() {
			return group;
		}

		public String getRoot() {
			return root;
		}

		public String getServers() {
			return this.servers + (null == this.root ? "" : "/" + this.root);
		}

		public int getTopicPartitions() {
			return topicPartitions;
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
		private MongoClientURI uri;

		private MongoClient client;
		private String db;
		private DB mongo;
		private Jongo jongo;

		@SuppressWarnings("deprecation")
		public MongoDataSource(
				String uri/* , String authDB, String authMechanism */) {
			super(Type.MONGODB, new MongoMarshaller());
			this.uri = new MongoClientURI(uri);// b.build();

			this.client = new MongoClient(this.uri);
			this.db = this.uri.getDatabase();
			this.mongo = this.client.getDB(db);
			this.jongo = new Jongo(this.mongo);
		}

		@Override
		public String toString() {
			return super.toString() + ":" + this.uri + "." + db;
		}

		public MongoClientURI getUri() {
			return uri;
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
