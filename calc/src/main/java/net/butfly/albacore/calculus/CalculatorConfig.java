package net.butfly.albacore.calculus;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.butfly.albacore.calculus.datasource.CalculatorDataSource;

public class CalculatorConfig {
	public JavaSparkContext sc;
	public JavaStreamingContext ssc;
	// public SQLContext sqsc;

	public Map<String, CalculatorDataSource> datasources = new HashMap<>();
	// public Map<String, KafkaConfig> kafkas = new HashMap<>();
	// public Map<String, HbaseConfig> hbases = new HashMap<>();
	// public Map<String, MongodbConfig> mongodbs = new HashMap<>();
	public boolean validate;

}
