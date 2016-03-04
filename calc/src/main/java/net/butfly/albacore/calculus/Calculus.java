package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface Calculus extends Serializable {
	JavaRDD<? extends Functor<?>> calculate(final JavaSparkContext sc,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stocking,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streaming);
}
