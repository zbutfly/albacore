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

	public PairRDS<K, V> clone() {
		PairRDS<K, V> rds = new PairRDS<>();
		rds.rdd = this.rdd;
		rds.dstream = this.dstream;
		return rds;
	}

	public boolean isEmpty() {
		return null != rdd ? rdd.isEmpty() : false;
	}

	public void foreachRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		// TODO Auto-generated method stub
	}

	public <K2, V2> PairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		// TODO Auto-generated method stub
		return null;
	}

	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		// TODO Auto-generated method stub
		return null;
	}

	public <W> PairRDS<K, Tuple2<V, W>> join(PairRDS<K, W> other) {
		// TODO Auto-generated method stub
		return null;
	}

	public <W> PairRDS<K, Tuple2<V, Optional<W>>> leftOuterJoin(PairRDS<K, W> other) {
		// TODO Auto-generated method stub
		return null;
	}

	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}
}
