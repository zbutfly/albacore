package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.jcabi.log.Logger;

import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.functor.Functors;

public interface Calculus<OUTK, OUTV extends Functor<OUTV>> extends Serializable {
	void stocking(final JavaSparkContext sc, final Functors<OUTK> functors, final VoidFunction<JavaPairRDD<OUTK, OUTV>> handler);

	void streaming(final JavaStreamingContext ssc, final Functors<OUTK> functors, final VoidFunction<JavaPairRDD<OUTK, OUTV>> handler);

	default boolean saving(JavaPairRDD<OUTK, OUTV> r) {
		return true;
	}

	default void traceCount(JavaPairRDD<?, ?> rdd, String prefix) {
		if (Logger.isTraceEnabled(this.getClass())) Logger.trace(this.getClass(), prefix + rdd.count());
	}

	default void traceCount(JavaPairRDD<?, ?> rdd, String prefix, double sd) {
		if (Logger.isTraceEnabled(this.getClass())) Logger.trace(this.getClass(), prefix + rdd.countApproxDistinct(sd));
	}

	@SuppressWarnings("deprecation")
	default void traceCount(JavaPairDStream<?, ?> stream, String prefix) {
		if (Logger.isTraceEnabled(this.getClass())) stream.count().foreachRDD(rdd -> {
			Logger.trace(this.getClass(), prefix + rdd.reduce((c1, c2) -> c1 + c2));
			return null;
		});
	}

	default <K, V> void traceInfo(JavaPairRDD<K, V> rdd, Function2<K, V, String> func) {
		if (Logger.isTraceEnabled(this.getClass())) rdd.foreach(t -> {
			Logger.trace(this.getClass(), func.call(t._1, t._2));
		});
	}

	@SuppressWarnings("deprecation")
	default <K, V> void traceInfo(JavaPairDStream<K, V> stream, Function2<K, V, String> func) {
		if (Logger.isTraceEnabled(this.getClass())) stream.foreachRDD(rdd -> {
			traceInfo(rdd, func);
			return null;
		});
	}
}
