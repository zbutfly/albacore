package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaReloadPairDStream<K, V> extends JavaWrappedPairInputDStream<K, V, RefreshableInputDStream<K, V>> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaReloadPairDStream(JavaStreamingContext ssc, Function0<JavaPairRDD<K, V>> loader, Class<K> kClass, Class<V> vClass) {
		super(new RefreshableInputDStream<K, V>(ssc, loader, kClass, vClass), kClass, vClass);
	}

}
