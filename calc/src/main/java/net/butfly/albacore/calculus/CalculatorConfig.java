package net.butfly.albacore.calculus;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.jongo.Jongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class CalculatorConfig {
	public JavaSparkContext sc;
	public JavaStreamingContext ssc;
	// public SQLContext sqsc;

	public Map<String, KafkaConfig> kafkas = new HashMap<>();
	public Map<String, HbaseConfig> hbases = new HashMap<>();
	public Map<String, MongodbConfig> mongodbs = new HashMap<>();
	public boolean validate;

	public static class KafkaConfig {
		public String quonum;
		public String group;
	}

	public static class HbaseConfig {
		public String config;
	}

	public static class MongodbConfig {
		public String uri;
		public String authuri;
		public String db;
		public MongoClient client;
		public DB mongo;
		public Jongo jongo;
	}
}
