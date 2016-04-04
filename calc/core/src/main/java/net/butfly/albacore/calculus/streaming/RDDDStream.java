package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.InputDStream;

import net.butfly.albacore.calculus.factor.rds.RDS;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction0;

public abstract class RDDDStream<T> extends InputDStream<T> {
	public enum Mechanism {
		CONST, STOCK, FRESH, @Deprecated BATCH, @Deprecated CACHE
	}

	protected SparkContext sc;
	protected RDD<T> current;

	RDDDStream(StreamingContext ssc) {
		super(ssc, RDS.tag());
		this.sc = ssc.sc();
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

	public static <K, V> JavaPairDStream<K, V> pstream(StreamingContext ssc, Mechanism mechanism, Function0<JavaPairRDD<K, V>> rdd) {
		try {
			return JavaPairDStream.fromPairDStream(new RDDInputDStream<Tuple2<K, V>>(ssc, mechanism, () -> rdd.call().map(t -> t).rdd()),
					RDS.tag(), RDS.tag());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Deprecated
	public static <K, V> JavaPairDStream<K, V> bpstream(StreamingContext ssc, long batch, Function2<Long, K, JavaPairRDD<K, V>> batcher,
			Comparator<K> comparator) {
		try {
			return JavaPairDStream.fromPairDStream(
					new RDDBatchInputDStream<K, V>(ssc, batch, (limit, offset) -> batcher.call(limit, offset).rdd(), comparator), RDS.tag(),
					RDS.tag());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static <R> JavaDStream<R> stream(StreamingContext ssc, Mechanism mechanism, Function0<JavaRDD<R>> rdd) {
		return JavaDStream.fromDStream(new RDDInputDStream<R>(ssc, mechanism, () -> rdd.call().rdd()), RDS.tag());
	}

	@SafeVarargs
	public static <R> JavaRDD<R> rdd(SparkContext sc, R... r) {
		return JavaRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(Arrays.asList(r)).seq(), sc.defaultParallelism(), RDS.tag()),
				RDS.tag());
	}

	@SuppressWarnings("unchecked")
	public static <R> JavaDStream<R> stream(StreamingContext ssc, R... r) {
		JavaRDD<R> rdd = rdd(ssc.sc(), r);
		return stream(ssc, Mechanism.CONST, () -> rdd);
	}
}