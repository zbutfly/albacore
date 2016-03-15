package net.butfly.albacore.calculus.functor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

@SuppressWarnings("unchecked")
public final class Functors<K> implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	private Map<Class<? extends Functor<?>>, JavaPairDStream<K, ? extends Functor<?>>> streamings = new HashMap<>();
	private Map<Class<? extends Functor<?>>, JavaPairRDD<K, ? extends Functor<?>>> stocking = new HashMap<>();

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

	public static <K, F extends Functor<F>> JavaPairDStream<K, F> streamize(JavaStreamingContext ssc, JavaPairRDD<K, F> rdd) {
		Queue<JavaRDD<Tuple2<K, F>>> q = new ArrayBlockingQueue<>(1);
		q.add(rdd.map(t -> t));
		return ssc.queueStream(q).mapToPair(t -> t).cache();
	}

}
