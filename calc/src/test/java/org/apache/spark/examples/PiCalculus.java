package org.apache.spark.examples;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.jcabi.log.Logger;

import net.butfly.albacore.calculus.Calculating;
import net.butfly.albacore.calculus.Calculus;
import net.butfly.albacore.calculus.Functor;
import scala.Tuple2;

@Calculating(stocking = { PiFunctor.class }, saving = PiFunctor.class)
public class PiCalculus implements Calculus {
	private static final long serialVersionUID = 3015356422557570388L;

	@SuppressWarnings({ "serial", "unchecked" })
	@Override
	public JavaRDD<? extends Functor<?>> calculate(JavaSparkContext sc,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stocking,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streaming) {
		JavaPairRDD<String, PiFunctor> rdds = (JavaPairRDD<String, PiFunctor>) stocking.get(PiFunctor.class);
		PiFunctor pi = new PiFunctor("0");
		pi.pi = rdds.map(new Function<Tuple2<String, PiFunctor>, Integer>() {
			@Override
			public Integer call(Tuple2<String, PiFunctor> rdd) throws Exception {
				Logger.trace(PiCalculus.class, "Mapping: " + rdd._2.value);
				double x = Math.random() * 2 - 1;
				double y = Math.random() * 2 - 1;
				return (x * x + y * y < 1) ? 1 : 0;
			}
		}).reduce((a, b) -> a + b) * 4.0 / rdds.count();

		return sc.parallelize(Arrays.asList(pi));
	}
}
