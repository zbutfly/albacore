package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;

public class PairRDS<K, V> extends RDS<Tuple2<K, V>> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 7112147100603052906L;

	protected PairRDS() {}

	public PairRDS(JavaPairRDD<K, V> rdd) {
		super(new WDD<Tuple2<K, V>>(rdd));
	}

	public PairRDS(JavaPairDStream<K, V> dstream) {
		super(new WStream<Tuple2<K, V>>(dstream));
	}

	protected PairRDS(Wrapped<Tuple2<K, V>> wrapped) {
		super(wrapped);
	}

	@SafeVarargs
	public PairRDS(JavaSparkContext sc, Tuple2<K, V>... t) {
		super(sc, t);
	}

	@Override
	public <RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, DataDetail<V> dd) {
		foreachPairRDD(rdd -> {
			JavaPairRDD<WK, WV> w = rdd.mapToPair(t -> (Tuple2<WK, WV>) ds.beforeWriting(t._1, t._2))
					.filter(t -> t != null && t._1 != null && t._2 != null);
			ds.save(w, dd);
		});
	}

	@Override
	public Map<K, V> collectAsMap() {
		Map<K, V> r = new HashMap<>();
		foreach((VoidFunction<Tuple2<K, V>>) t -> r.put(t._1, t._2));
		return r;
	}

	@Override
	public List<K> collectKeys() {
		return map(t -> t._1).collect();
	}

	@Override
	public Collection<JavaPairRDD<K, V>> pairRDDs() {
		return Reflections.transform(wrapped().rdds(), rdd -> JavaPairRDD.fromRDD(rdd, k(), v()));
	}

	@Override
	public JavaPairRDD<K, V> pairRDD() {
		return JavaPairRDD.fromRDD(wrapped().rdd(), k(), v());
	}

	@Override
	public void foreachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		wrapped.foreachRDD(rdd -> consumer.call(JavaPairRDD.fromJavaRDD(rdd)));
	}

	@Override
	public void foreach(VoidFunction2<K, V> consumer) {
		wrapped.foreach(t -> consumer.call(t._1, t._2));
	}

	@Override
	public PairRDS<K, V> unpersist() {
		return new PairRDS<>(wrapped().unpersist());
	}

	@Override
	public PairRDS<K, V> persist() {
		return new PairRDS<>(wrapped().persist());
	}

	@Override
	public PairRDS<K, V> persist(StorageLevel level) {
		return new PairRDS<>(wrapped().persist(level));
	}

	@Override
	public PairRDS<K, V> repartition(float ratioPartitions, boolean rehash) {
		if (!rehash) return repartition(ratioPartitions);
		switch (mode()) {
		case STOCKING:
			return new PairRDS<K, V>(JavaPairRDD.fromRDD(rdd(), k(), v())
					.partitionBy(new HashPartitioner((int) Math.ceil(getNumPartitions() * ratioPartitions))));
		case STREAMING:
			JavaPairDStream<K, V> ds = JavaPairDStream.fromPairDStream(dstream(Wrapped.ssc(this)), k(), v())
					.transformToPair((Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>>) rdd -> rdd
							.partitionBy(new HashPartitioner((int) Math.ceil(rdd.getNumPartitions() * ratioPartitions))));
			return new PairRDS<K, V>(ds);
		default:
			throw new IllegalArgumentException();
		}
	}

	@Override
	public PairRDS<K, V> repartition(float ratioPartitions) {
		return new PairRDS<K, V>(wrapped().repartition(ratioPartitions));
	}

	@Override
	public Tuple2<K, V> first() {
		Collection<RDD<Tuple2<K, V>>> rs = rdds();
		return rs.size() > 0 ? rs.iterator().next().first() : null;
	}

	@Override
	public K maxKey() {
		@SuppressWarnings("unchecked")
		Ordering<K> c = (Ordering<K>) Ordering.natural();
		return this.reduce((t1, t2) -> (c.compare(t1._1, t2._1) > 0 ? t1 : t2))._1;
	}

	@Override
	public K minKey() {
		@SuppressWarnings("unchecked")
		Ordering<K> c = (Ordering<K>) Ordering.natural();
		return this.reduce((t1, t2) -> (c.compare(t1._1, t2._1) < 0 ? t1 : t2))._1;
	}

	@Override
	public PairRDS<K, V> sortByKey(boolean asc) {
		return new PairRDS<>(pairRDD().sortByKey(asc));
	}

	@Override
	public <S> PairRDS<K, V> sortBy(Function2<K, V, S> comp) {
		return new PairRDS<>(wrapped().sortBy(t -> comp.call(t._1, t._2)));
	}

	public PairRDS<K, V> union(PairWrapped<K, V> other) {
		return new PairRDS<K, V>(wrapped.union(other));
	}

	@Override
	public PairRDS<K, V> union(Wrapped<Tuple2<K, V>> other) {
		return new PairRDS<>(wrapped().union(other));
	}

	@Override
	public PairRDS<K, V> filter(Function2<K, V, Boolean> func) {
		return new PairRDS<>(filter(t -> func.call(t._1, t._2)).wrapped());
	}

	@Override
	public PairRDS<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		return new PairRDS<>(wrapped().filter(func));
	}

	@Override
	public final <K2, V2> PairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		switch (mode()) {
		case STOCKING:
			return new PairRDS<K2, V2>(JavaPairRDD.fromRDD(rdd(), k(), v()).mapToPair(func::call));
		case STREAMING:
			return new PairRDS<K2, V2>(JavaPairDStream.fromPairDStream(dstream(Wrapped.ssc(this)), k(), v()).mapToPair(func::call));
		default:
			throw new IllegalArgumentException();
		}
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other) {
		if (mode() == Mode.STOCKING && other.mode() == Mode.STOCKING) return new PairRDS<>(
				JavaPairRDD.fromRDD(rdd(), k(), v()).join(JavaPairRDD.fromRDD(other.rdd(), RDSupport.tag(), RDSupport.tag())));
		else {
			StreamingContext ssc = Wrapped.ssc(this, other);
			return new PairRDS<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
					.join(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag())));
		}
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions) {
		int pnum = (int) Math.ceil(Math.max(getNumPartitions(), other.getNumPartitions()) * ratioPartitions);
		// two stream, use 10 for devel testing.
		if (pnum <= 0) pnum = ((int) Math.ceil(ratioPartitions)) * 10;
		if (mode() == Mode.STOCKING && other.mode() == Mode.STOCKING) {
			return new PairRDS<>(pairRDD().join(JavaPairRDD.fromRDD(other.rdd(), RDSupport.tag(), RDSupport.tag()), pnum));
		} else {
			StreamingContext ssc = Wrapped.ssc(this, other);
			return new PairRDS<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
					.join(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag()), pnum));
		}
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other) {
		if (mode() == Mode.STOCKING && other.mode() == Mode.STOCKING) return new PairRDS<>(
				JavaPairRDD.fromRDD(rdd(), k(), v()).leftOuterJoin(JavaPairRDD.fromRDD(other.rdd(), RDSupport.tag(), RDSupport.tag())));
		else {
			StreamingContext ssc = Wrapped.ssc(this, other);
			return new PairRDS<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
					.leftOuterJoin(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag())));
		}
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, int numPartitions) {
		if (mode() == Mode.STOCKING && other.mode() == Mode.STOCKING) return new PairRDS<>(
				JavaPairRDD.fromRDD(rdd(), k(), v()).leftOuterJoin(JavaPairRDD.fromRDD(other.rdd(), RDSupport.tag(), RDSupport.tag())));
		else {
			StreamingContext ssc = Wrapped.ssc(this, other);
			return new PairRDS<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
					.leftOuterJoin(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag())));
		}
	}

	@Override
	public <U> PairRDS<K, Iterable<V>> groupByKey() {
		switch (mode()) {
		case STOCKING:
			List<JavaPairRDD<K, Iterable<V>>> l = new ArrayList<>();
			foreachPairRDD(rdd -> l.add(rdd.groupByKey()));
			return new PairRDS<K, Iterable<V>>(RDSupport.union(l));
		case STREAMING:
			return new PairRDS<K, Iterable<V>>(
					JavaPairDStream.fromPairDStream(((WStream<Tuple2<K, V>>) wrapped()).dstream, k(), v()).groupByKey());
		default:
			throw new IllegalArgumentException();
		}
	}

	@Override
	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		switch (mode()) {
		case STOCKING:
			List<JavaPairRDD<K, V>> l = new ArrayList<>();
			foreachPairRDD(rdd -> l.add(rdd.reduceByKey(func)));
			return new PairRDS<K, V>(RDSupport.union(l));
		case STREAMING:
			DStream<Tuple2<K, V>> ds = ((WStream<Tuple2<K, V>>) wrapped()).dstream;
			return new PairRDS<K, V>(JavaPairDStream.fromPairDStream(ds, k(), v()).reduceByKey(func));
		default:
			throw new IllegalArgumentException();
		}
	}

	@Override
	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions) {
		int pnum = (int) Math.ceil(getNumPartitions() * ratioPartitions);
		// two stream, use 10 for devel testing.
		int minpnum = pnum < 0 ? ((int) Math.ceil(ratioPartitions)) * 10 : pnum;
		switch (mode()) {
		case STOCKING:
			List<JavaPairRDD<K, V>> l = new ArrayList<>();
			foreachPairRDD(rdd -> l.add(rdd.reduceByKey(func, minpnum)));
			return new PairRDS<K, V>(RDSupport.union(l));
		case STREAMING:
			DStream<Tuple2<K, V>> ds = ((WStream<Tuple2<K, V>>) wrapped()).dstream;
			return new PairRDS<K, V>(JavaPairDStream.fromPairDStream(ds, k(), v()).reduceByKey(func, minpnum));
		default:
			throw new IllegalArgumentException();
		}
	}
}
