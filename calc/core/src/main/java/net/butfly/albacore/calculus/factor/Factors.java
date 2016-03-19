package net.butfly.albacore.calculus.factor;

import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.butfly.albacore.calculus.streaming.JavaConstPairDStream;

@SuppressWarnings("unchecked")
public final class Factors extends HashMap<Class<? extends Factor<?>>, JavaPairDStream<?, ? extends Factor<?>>> {
	private static final long serialVersionUID = -3712903710207597570L;

	public <K, F extends Factor<F>> void streaming(Class<F> factor, JavaPairDStream<K, F> ds) {
		this.put(factor, ds);
	}

	public <K, F extends Factor<F>> void stocking(Class<F> factor, JavaPairRDD<K, F> rdd, JavaStreamingContext ssc) {
		this.put(factor, new JavaConstPairDStream<>(ssc, rdd));
	}

	public <K, F extends Factor<F>> JavaPairDStream<K, F> streaming(Class<F> factor) {
		return (JavaPairDStream<K, F>) get(factor);
	}

	public <K, F extends Factor<F>> JavaPairDStream<K, F> stocking(Class<F> factor) {
		return (JavaPairDStream<K, F>) get(factor);
	}
}
