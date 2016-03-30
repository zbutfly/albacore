package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.google.common.base.Optional;

import scala.Tuple2;

public class PairRDS<K, V> {
	JavaPairRDD<K, V> rdd = null;
	JavaPairDStream<K, V> dstream = null;

	private PairRDS() {}

	public PairRDS(JavaPairDStream<K, V> dstream) {
		this.dstream = dstream;
	}

	public PairRDS(JavaPairRDD<K, V> rdd) {
		this.rdd = rdd;
	}

	@SafeVarargs
	public PairRDS(JavaSparkContext sc, Tuple2<K, V>... t) {
		this(sc.parallelize(Arrays.asList(t)).mapToPair(tt -> tt));
	}

	public PairRDS(JavaSparkContext sc, Map<K, V> m) {
		this(sc.parallelize(new ArrayList<>(m.entrySet())).map(e -> new Tuple2<K, V>(e.getKey(), e.getValue())).mapToPair(tt -> tt));
	}

	public PairRDS<K, V> folk() {
		if (null == rdd) rdd = rdd.cache();
		else dstream = dstream.cache();
		PairRDS<K, V> rds = new PairRDS<>();
		rds.rdd = rdd;
		rds.dstream = dstream;
		return rds;
	}

	public boolean isEmpty() {
		return null != rdd ? rdd.isEmpty() : false;
	}

	@SuppressWarnings("deprecation")
	public void foreachRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		if (null == rdd) dstream.foreachRDD(rdd -> {
			consumer.call(rdd);
			return null;
		});
		else try {
			consumer.call(rdd);
		} catch (Exception e) {
			throw new RuntimeException("foreachRDD callback failure", e);
		}
	}

	public <K2, V2> PairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		return null == rdd ? new PairRDS<K2, V2>(dstream.mapToPair(func)) : new PairRDS<K2, V2>(rdd.mapToPair(func));
	}

	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		return null == rdd ? new PairRDS<K, V>(dstream.reduceByKey(func)) : new PairRDS<K, V>(rdd.reduceByKey(func));
	}

	public <W> PairRDS<K, Tuple2<V, W>> join(PairRDS<K, W> other) {
		return null == rdd ? new PairRDS<K, Tuple2<V, W>>(dstream.join(other.dstream)) : new PairRDS<K, Tuple2<V, W>>(rdd.join(other.rdd));
	}

	public <W> PairRDS<K, Tuple2<V, Optional<W>>> leftOuterJoin(PairRDS<K, W> other) {
		return null == rdd ? new PairRDS<K, Tuple2<V, Optional<W>>>(dstream.leftOuterJoin(other.dstream))
				: new PairRDS<K, Tuple2<V, Optional<W>>>(rdd.leftOuterJoin(other.rdd));
	}

	@SuppressWarnings("deprecation")
	public long count() {
		if (null != rdd) return rdd.count();
		else {
			long[] r = new long[] { 0 };
			dstream.count().foreach(rl -> {
				for (long l : rl.collect())
					r[0] += l;
				return null;
			});
			return r[0];
		}
	}
}
