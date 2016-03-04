package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.jongo.Jongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import net.butfly.albacore.calculus.marshall.Marshaller;

public abstract class CalculatorDataSource implements Serializable {
	private static final long serialVersionUID = 1L;
	public Marshaller<?, ?> marshaller;

	public static class KafkaDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = 7500441385655250814L;
		public String quonum;
		public String group;
	}

	public static class HbaseDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = 3367501286179801635L;
		public String configFile;
		public Connection hconn;
	}

	public static class MongoDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = -2617369621178264387L;
		public Configuration mconf;
		public String uri;
		public String authuri;
		public String db;
		public MongoClient client;
		public DB mongo;
		public Jongo jongo;
	}

	public static class ConstDataSource extends CalculatorDataSource {
		private static final long serialVersionUID = -673387208224779163L;
		public String[] values;
	}
}
