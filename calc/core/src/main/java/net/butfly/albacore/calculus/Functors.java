package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;

@SuppressWarnings("unchecked")
public final class Functors<K> implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	private Map<Class<? extends Functor<?>>, JavaPairDStream<K, ? extends Functor<?>>> streamings = new HashMap<>();
	private Map<Class<? extends Functor<?>>, JavaPairRDD<K, ? extends Functor<?>>> stocking = new HashMap<>();

	public <F extends Functor<F>> void streaming(Class<F> functor, JavaPairDStream<K, F> ds) {
		this.streamings.put(functor, ds);
	}

	public <F extends Functor<F>> void streaming(Class<F> functor, JavaPairRDD<K, F> rdd) {
		this.streamings.put(functor, streamize(rdd));
	}

	public <F extends Functor<F>> void stocking(Class<F> functor, JavaPairRDD<K, F> rdd) {
		this.stocking.put(functor, rdd);
	}

	public <F extends Functor<F>> JavaPairDStream<K, F> streaming(Class<F> functor) {
		return (JavaPairDStream<K, F>) streamings.get(functor);
	}

	public <F extends Functor<F>> JavaPairRDD<K, F> stocking(Class<F> functor) {
		return (JavaPairRDD<K, F>) stocking.get(functor);
	}

	// TODO
	public static <K, F extends Functor<F>> JavaPairDStream<K, F> streamize(JavaPairRDD<K, F> rdd) {
		return null;
	}
}
