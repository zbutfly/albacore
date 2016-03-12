package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;

@SuppressWarnings("unchecked")
public final class Functors implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	private Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stockings = new HashMap<>();
	private Map<Class<? extends Functor<?>>, JavaPairDStream<String, ? extends Functor<?>>> streamings = new HashMap<>();

	public <F extends Functor<F>> void add(Class<? extends Functor<?>> f, JavaPairRDD<String, F> rdd) {
		this.stockings.put(f, rdd);
	}

	public <F extends Functor<F>> void add(Class<? extends Functor<?>> functor, JavaPairDStream<String, F> rdd) {
		this.streamings.put(functor, rdd);
	}

	public <F extends Functor<F>> JavaPairRDD<String, F> stocking(Class<F> functor) {
		return (JavaPairRDD<String, F>) stockings.get(functor);
	}

	public <F extends Functor<F>> JavaPairDStream<String, F> streaming(Class<F> functor) {
		return (JavaPairDStream<String, F>) streamings.get(functor);
	}
}
