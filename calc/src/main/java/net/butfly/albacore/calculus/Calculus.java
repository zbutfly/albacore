package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Calculus implements Serializable {
	private static final long serialVersionUID = 1L;
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	abstract protected JavaRDD<? extends Functor<?>> calculate(final JavaSparkContext sc,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stocking,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streaming);
}
