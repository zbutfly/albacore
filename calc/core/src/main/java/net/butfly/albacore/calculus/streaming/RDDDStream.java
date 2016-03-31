package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;

import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction0;

public abstract class RDDDStream<T> extends InputDStream<T> {
	public enum Mechanism {
		CONST, STOCK, FRESH, @Deprecated BATCH, @Deprecated CACHE
	}

	protected SparkContext sc;
	protected RDD<T> current;
	protected ClassTag<T> classTag;

	RDDDStream(StreamingContext ssc, ClassTag<T> classTag) {
		super(ssc, classTag);
		this.sc = ssc.sc();
		this.classTag = classTag;
	}

	abstract protected RDD<T> load();

	public Option<RDD<T>> compute(Time time) {
		trace(() -> "RDD inputted as streaming with count: " + current.count());
		return Option.apply(current);
	}

	@Override
	final public String name() {
		return super.name() + "[for " + (null == current ? "null" : current.toDebugString()) + "]";
	}

	@Override
	final public void start() {}

	@Override
	final public void stop() {}

	final protected void trace(Function0<String> msg) {
		logTrace(new AbstractFunction0<String>() {
			@Override
			public String apply() {
				try {
					return msg.call();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	public static <K, V> JavaPairDStream<K, V> pstream(JavaStreamingContext ssc, Mechanism mechanism, Function0<JavaPairRDD<K, V>> rdd,
			Class<K> kc, Class<V> vc) {
		try {
			return JavaPairDStream.fromPairDStream(
					new RDDInputDStream<Tuple2<K, V>>(ssc.ssc(), mechanism, () -> rdd.call().map(t -> t).rdd(),
							scala.reflect.ClassTag$.MODULE$.<Tuple2<K, V>> apply(Tuple2.class)),
					scala.reflect.ClassTag$.MODULE$.<K> apply(kc), scala.reflect.ClassTag$.MODULE$.<V> apply(vc));
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Deprecated
	public static <K, V> JavaPairDStream<K, V> bpstream(JavaStreamingContext ssc, long batch, Function2<Long, K, JavaPairRDD<K, V>> batcher,
			Comparator<K> comparator, Class<K> kc, Class<V> vc) {
		try {
			return JavaPairDStream.fromPairDStream(
					new RDDBatchInputDStream<K, V>(ssc.ssc(), batch, (limit, offset) -> batcher.call(limit, offset).rdd(), comparator,
							scala.reflect.ClassTag$.MODULE$.<Tuple2<K, V>> apply(Tuple2.class)),
					scala.reflect.ClassTag$.MODULE$.<K> apply(kc), scala.reflect.ClassTag$.MODULE$.<V> apply(vc));
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static <R> JavaDStream<R> stream(JavaStreamingContext ssc, Mechanism mechanism, Function0<JavaRDD<R>> rdd, Class<R> rc) {
		ClassTag<R> r = scala.reflect.ClassTag$.MODULE$.<R> apply(rc);
		return JavaDStream.fromDStream(new RDDInputDStream<R>(ssc.ssc(), mechanism, () -> rdd.call().rdd(), r), r);
	}

	@SafeVarargs
	public static <R> JavaRDD<R> rdd(JavaSparkContext sc, R... r) {
		return sc.parallelize(Arrays.asList(r));
	}

	@SuppressWarnings("unchecked")
	public static <R> JavaDStream<R> stream(JavaStreamingContext ssc, R... r) {
		JavaRDD<R> rdd = rdd(ssc.sparkContext(), r);
		return stream(ssc, Mechanism.CONST, () -> rdd, (Class<R>) rdd.classTag().runtimeClass());
	}
}