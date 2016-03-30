package net.butfly.albacore.calculus.streaming.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaFreshPairDStream<K, V> extends JavaWrappedPairInputDStream<K, V, FreshInputDStream<K, V>> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaFreshPairDStream(JavaStreamingContext ssc, Function0<JavaPairRDD<K, V>> loader, Class<K> kClass, Class<V> vClass) {
		super(new FreshInputDStream<K, V>(ssc, loader, kClass, vClass), kClass, vClass);
	}

}
