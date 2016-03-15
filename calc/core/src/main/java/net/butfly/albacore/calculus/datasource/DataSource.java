package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Connection;

import com.google.common.base.Joiner;

import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.functor.Functor.Type;
import net.butfly.albacore.calculus.marshall.HbaseMarshaller;
import net.butfly.albacore.calculus.marshall.KafkaMarshaller;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;

public abstract class DataSource implements Serializable {
	private static final long serialVersionUID = 1L;
	Functor.Type type;
	Marshaller<?, ?> marshaller;

	public Functor.Type getType() {
		return type;
	}

	public Marshaller<?, ?> getMarshaller() {
		return marshaller;
	}

	public DataSource(Type type, Marshaller<?, ?> marshaller) {
		super();
		this.type = type;
		this.marshaller = marshaller;
	}

	@Override
	public String toString() {
		return "CalculatorDataSource:" + this.type;
	}

	public static class KafkaDataSource extends DataSource {
		private static final long serialVersionUID = 7500441385655250814L;
		String servers;
		String root;
		String group;
		int topicPartitions;

		public KafkaDataSource(String servers, String root, int topicPartitions, String group, Marshaller<?, ?> marshaller) {
			super(Type.KAFKA, null == marshaller ? new KafkaMarshaller() : marshaller);
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

		public KafkaDataSource(String servers, String root, int topicPartitions, String group) {
			this(servers, root, topicPartitions, group, new KafkaMarshaller());
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

	public static class HbaseDataSource extends DataSource {
		private static final long serialVersionUID = 3367501286179801635L;
		String configFile;
		Connection hconn;

		public HbaseDataSource(String configFile, Marshaller<?, ?> marshaller) {
			super(Type.HBASE, null == marshaller ? new HbaseMarshaller() : marshaller);
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

	public static class MongoDataSource extends DataSource {
		private static final long serialVersionUID = -2617369621178264387L;
		String uri;

		public MongoDataSource(String uri, Marshaller<?, ?> marshaller) {
			super(Type.MONGODB, null == marshaller ? new MongoMarshaller() : marshaller);
			this.uri = uri;
		}

		public MongoDataSource(String uri) {
			this(uri, new MongoMarshaller());
		}

		@Override
		public String toString() {
			return super.toString() + ":" + this.uri;
		}

		public String getUri() {
			return uri;
		}
	}

	public static class ConstDataSource extends DataSource {
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
