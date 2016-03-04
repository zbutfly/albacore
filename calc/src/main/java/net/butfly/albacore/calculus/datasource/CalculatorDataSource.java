package net.butfly.albacore.calculus.datasource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.jongo.Jongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import net.butfly.albacore.calculus.marshall.Marshaller;

public abstract class CalculatorDataSource {
	public Marshaller<?, ?> marshaller;

	public static class KafkaDataSource extends CalculatorDataSource {
		public String quonum;
		public String group;
	}

	public static class HbaseDataSource extends CalculatorDataSource {
		public String configFile;
		public Connection hconn;
	}

	public static class MongoDataSource extends CalculatorDataSource {
		public Configuration mconf;
		public String uri;
		public String authuri;
		public String db;
		public MongoClient client;
		public DB mongo;
		public Jongo jongo;
	}

	public static class ConstDataSource extends CalculatorDataSource {
		public String[] values;
	}
}
