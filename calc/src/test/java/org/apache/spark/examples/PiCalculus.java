package org.apache.spark.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.butfly.albacore.calculus.Calculating;
import net.butfly.albacore.calculus.Calculus;
import net.butfly.albacore.calculus.Functor;

@Calculating(stocking = { PiFunctor.class }, saving = PiFunctor.class)
public class PiCalculus extends Calculus {
	private static final long serialVersionUID = 3015356422557570388L;

	@Override
	protected JavaRDD<? extends Functor<?>> calculate(JavaSparkContext sc,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stocking,
			Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> streaming) {
		final int slices = 2;
		final int n = 100000 * slices;
		final List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		final JavaRDD<Integer> dataSet = sc.parallelize(l, slices);

		JavaRDD<Integer> m = dataSet.map(integer -> {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y < 1) ? 1 : 0;
		});

		final int count = m.reduce((a, b) -> a + b);

		System.out.println("Pi is roughly " + 4.0 * count / n);
		return null;
	}
}
