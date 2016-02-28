package net.butfly.albacore.calculus;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CalculatorConfig {
	public JavaSparkContext sc;
	public SQLContext sqsc;
	public JavaStreamingContext ssc;
	public String hconfig;
	public String kquonum;
	public String kgroup;
}
