package net.butfly.albacore.calculus;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.butfly.albacore.calculus.datasource.DataSource;

abstract class CalculatorContext {
	public Map<String, DataSource> datasources = new HashMap<>();
	public boolean validate;

	static class StockingContext extends CalculatorContext {
		JavaSparkContext sc;
	}

	static class StreamingContext extends CalculatorContext {
		StreamingContext(StockingContext sc, int duration) {
			this.validate = sc.validate;
			this.datasources.putAll(sc.datasources);
			this.ssc = new JavaStreamingContext(sc.sc, Durations.seconds(duration));
		}

		JavaStreamingContext ssc;
	}
}
