package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public interface Calculus<OUT extends Functor<OUT>> extends Serializable {
	@SuppressWarnings("unchecked")
	void calculate(final JavaSparkContext sc, Functors functors, Function<JavaRDD<OUT>, Void>... handler) throws Exception;
}
