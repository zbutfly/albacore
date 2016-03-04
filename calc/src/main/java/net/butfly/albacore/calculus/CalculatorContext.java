package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.butfly.albacore.calculus.datasource.CalculatorDataSource;

public abstract class CalculatorContext implements Serializable {
	private static final long serialVersionUID = 1L;
	public Map<String, CalculatorDataSource> datasources = new HashMap<>();
	public boolean validate;

	public static class SparkCalculatorContext extends CalculatorContext {
		private static final long serialVersionUID = -946869209567800741L;
		public JavaSparkContext sc;
		public JavaStreamingContext ssc;
		// public SQLContext sqsc;
	}
}
