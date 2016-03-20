package net.butfly.albacore.calculus.factor;

import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.butfly.albacore.calculus.streaming.JavaConstPairDStream;

@SuppressWarnings("unchecked")
public final class Factors extends HashMap<Class<? extends Factor<?>>, JavaPairInputDStream<?, ? extends Factor<?>>> {
	private static final long serialVersionUID = -3712903710207597570L;

	public <K, F extends Factor<F>> void streaming(Class<F> factor, JavaPairInputDStream<K, F> ds) {
		this.put(factor, ds);
	}

	public <K, F extends Factor<F>> void stocking(Class<F> factor, JavaPairRDD<K, F> rdd, JavaStreamingContext ssc) {
		rdd.setName("RDD [" + factor.toString() + "]");
		this.put(factor, new JavaConstPairDStream<>(ssc, rdd));
	}

	public <K, F extends Factor<F>> JavaPairInputDStream<K, F> streaming(Class<F> factor) {
		return (JavaPairInputDStream<K, F>) get(factor);
	}

	public <K, F extends Factor<F>> JavaPairInputDStream<K, F> stocking(Class<F> factor) {
		return (JavaPairInputDStream<K, F>) get(factor);
	}
}
