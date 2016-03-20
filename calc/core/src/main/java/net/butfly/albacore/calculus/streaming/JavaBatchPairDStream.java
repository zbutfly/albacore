package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaBatchPairDStream<K, V> extends JavaWrappedPairInputDStream<K, V, BatchInputDStream<K, V>> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaBatchPairDStream(JavaStreamingContext ssc, Function2<Long, K, JavaPairRDD<K, V>> batcher, long batching, Class<K> kClass,
			Class<V> vClass) {
		super(new BatchInputDStream<>(ssc, batcher, batching, kClass, vClass), kClass, vClass);
	}
}
