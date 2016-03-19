package net.butfly.albacore.calculus.factor;

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
public final class Factors implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	private Map<Class<? extends Factor<?>>, JavaPairRDD<?, ? extends Factor<?>>> stocking = new HashMap<>();
	private Map<Class<? extends Factor<?>>, JavaPairDStream<?, ? extends Factor<?>>> streamings = new HashMap<>();

	public <K, F extends Factor<F>> void streaming(Class<F> factor, JavaPairDStream<K, F> ds) {
		this.streamings.put(factor, ds);
	}

	public <K, F extends Factor<F>> void stocking(Class<F> factor, JavaPairRDD<K, F> rdd) {
		this.stocking.put(factor, rdd);
	}

	public <K, F extends Factor<F>> JavaPairDStream<K, F> streaming(Class<F> factor) {
		return (JavaPairDStream<K, F>) streamings.get(factor);
	}

	public <K, F extends Factor<F>> JavaPairRDD<K, F> stocking(Class<F> factor) {
		return (JavaPairRDD<K, F>) stocking.get(factor);
	}

	/**
	 * @param ssc
	 * @param rdd
	 * @return
	 * @deprecated by {@code JavaConstantPairDStream} or
	 *             {@code JavaRefreshablePairDStream}
	 */
	@Deprecated
	public static <K, F extends Factor<F>> JavaPairDStream<K, F> streamize(JavaStreamingContext ssc, JavaPairRDD<K, F> rdd) {
		return new JavaInputDStream<>(new ConstantInputDStream<Tuple2<K, F>>(ssc.ssc(), rdd.map(t -> t).rdd(), rdd.classTag()),
				rdd.classTag()).mapToPair(t -> t);
	}
}
