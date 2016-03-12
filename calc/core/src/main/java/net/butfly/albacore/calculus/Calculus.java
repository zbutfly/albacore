package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStreamLike;

public interface Calculus<OUT extends Functor<OUT>> extends Serializable {
	@SuppressWarnings("unchecked")
	void calculate(final JavaSparkContext sc, Functors functors, Function<JavaRDD<OUT>, Void>... handler) throws Exception;

	// deprecation for spark 1.5.x
	@SuppressWarnings("deprecation")
	static long count(JavaDStreamLike<?, ?, ?> streaming) {
		long[] i = new long[] { 0 };
		streaming.count().foreachRDD(rdd -> {
			i[0] += rdd.reduce((v1, v2) -> v1 + v2);
			return null;
		});
		return i[0];
	}
}
