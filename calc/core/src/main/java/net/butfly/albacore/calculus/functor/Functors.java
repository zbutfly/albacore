package net.butfly.albacore.calculus.functor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ConstantInputDStream;

import scala.Tuple2;

@SuppressWarnings("unchecked")
public final class Functors<K> implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	private Map<Class<? extends Functor<?>>, JavaPairRDD<K, ? extends Functor<?>>> stocking = new HashMap<>();
	private Map<Class<? extends Functor<?>>, JavaPairDStream<K, ? extends Functor<?>>> streamings = new HashMap<>();

	public <F extends Functor<F>> void streaming(Class<F> functor, JavaPairDStream<K, F> ds) {
		this.streamings.put(functor, ds);
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

	/**
	 * @param ssc
	 * @param rdd
	 * @return
	 * @deprecated by {@code JavaConstantPairDStream} or
	 *             {@code JavaRefreshablePairDStream}
	 */
	@Deprecated
	public static <K, F extends Functor<F>> JavaPairDStream<K, F> streamize(JavaStreamingContext ssc, JavaPairRDD<K, F> rdd) {
		return new JavaInputDStream<>(new ConstantInputDStream<Tuple2<K, F>>(ssc.ssc(), rdd.map(t -> t).rdd(), rdd.classTag()),
				rdd.classTag()).mapToPair(t -> t);
	}
}
