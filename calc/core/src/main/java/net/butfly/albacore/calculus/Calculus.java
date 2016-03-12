package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.jcabi.log.Logger;

public interface Calculus extends Serializable {
	default JavaRDD<? extends Functor<?>> calculate(final JavaSparkContext sc,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> functors) {
		for (Class<? extends Functor<?>> c : functors.keySet())
			Logger.trace(Calculus.class, "Calculus got [" + functors.get(c).count() + "] records of " + c.toString() + "...");
		return null;
	};

	default JavaRDD<? extends Functor<?>> calculates(final JavaSparkContext sc,
			Map<Class<? extends Functor<?>>, JavaPairDStream<String, ? extends Functor<?>>> dss) {
		for (Class<? extends Functor<?>> c : dss.keySet()) {
			long[] i = new long[] { 0 };
			dss.get(c).count().foreachRDD(rdd -> {
				i[0] += rdd.reduce((c1, c2) -> c1 + c2);
			});
			Logger.trace(Calculus.class, "Calculus got [" + i[0] + "] streaming records of " + c.toString() + "...");
		}
		return null;
	};

	@Deprecated
	static <F extends Functor<F>> JavaPairRDD<String, F> union(JavaPairDStream<String, F> dstream) {
		if (dstream == null) return null;
		@SuppressWarnings("unchecked")
		JavaPairRDD<String, F>[] r = new JavaPairRDD[] { null };
		dstream.foreachRDD(rdd -> {
			if (null == r[0]) r[0] = rdd;
			else r[0] = r[0].union(rdd);
		});
		return r[0];
	}

}
