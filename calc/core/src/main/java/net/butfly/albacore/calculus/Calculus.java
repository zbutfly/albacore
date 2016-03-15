package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.butfly.albacore.calculus.functor.Functor;
import net.butfly.albacore.calculus.functor.Functors;

public interface Calculus<OUTK, OUTV extends Functor<OUTV>> extends Serializable {
	void stocking(final JavaSparkContext sc, final Functors<OUTK> functors, final VoidFunction<JavaPairRDD<OUTK, OUTV>> handler);

	void streaming(final JavaStreamingContext ssc, final Functors<OUTK> functors, final VoidFunction<JavaPairRDD<OUTK, OUTV>> handler);

	default boolean saving(JavaPairRDD<OUTK, OUTV> r) {
		return true;
	}
}
