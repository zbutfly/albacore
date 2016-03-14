package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface Calculus<K, OUT extends Functor<OUT>> extends Serializable {
	void stocking(final JavaSparkContext sc, final Functors<K> functors, final VoidFunction<JavaRDD<OUT>> handler);

	void streaming(final JavaStreamingContext ssc, final Functors<K> functors, final VoidFunction<JavaRDD<OUT>> handler);

	default boolean saving(JavaRDD<OUT> r) {
		return true;
	}
}
