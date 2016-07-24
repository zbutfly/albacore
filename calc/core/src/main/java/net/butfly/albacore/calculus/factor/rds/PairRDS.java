package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;

import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.factor.rds.internal.PairWrapped;
import net.butfly.albacore.calculus.factor.rds.internal.RDSupport;
import net.butfly.albacore.calculus.factor.rds.internal.Wrapped;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDStream;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDataFrame;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDataset;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import scala.Tuple2;

/**
 * Single including any implementation of spark data.
 * 
 * @author zx
 *
 * @param <K>
 * @param <V>
 */
@SuppressWarnings("deprecation")
public final class PairRDS<K, V> extends RDS<Tuple2<K, V>> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 7112147100603052906L;

	protected PairRDS() {}

	public PairRDS(Wrapped<Tuple2<K, V>> wrapped) {
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
	public void foreachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		wrapped.foreachRDD(r -> consumer.call(JavaPairRDD.fromJavaRDD(r)));
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
		return isStream()
				? new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(dstream(Wrapped.streaming(this)), k(), v())
						.transformToPair((Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>>) rdd -> rdd
								.partitionBy(new HashPartitioner((int) Math.ceil(rdd.getNumPartitions() * ratioPartitions))))))
				: new PairRDS<>(new WrappedRDD<>(JavaPairRDD.fromJavaRDD(rdd().toJavaRDD())
						.partitionBy(new HashPartitioner((int) Math.ceil(getNumPartitions() * ratioPartitions)))));

	}

	@Override
	public PairRDS<K, V> repartition(float ratioPartitions) {
		return new PairRDS<>(wrapped().repartition(ratioPartitions));
	}

	@Override
	public PairRDS<K, V> sortByKey(boolean asc) {
		return new PairRDS<>(new WrappedRDD<>(pairRDD().sortByKey(asc)));
	}

	@Override
	public <S> PairRDS<K, V> sortBy(Function2<K, V, S> comp) {
		return new PairRDS<>(wrapped().sortBy(t -> comp.call(t._1, t._2)));
	}

	public PairRDS<K, V> union(PairWrapped<K, V> other) {
		return new PairRDS<>(wrapped.union(other));
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
		return isStream()
				? new PairRDS<>(new WrappedDStream<>(
						JavaPairDStream.fromPairDStream(dstream(Wrapped.streaming(this)), k(), v()).mapToPair(func::call)))
				: new PairRDS<>(new WrappedRDD<>(JavaPairRDD.fromJavaRDD(rdd().toJavaRDD()).mapToPair(func::call)));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other) {
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null == ssc) return new PairRDS<>(
				new WrappedRDD<>(JavaPairRDD.fromJavaRDD(rdd().toJavaRDD()).join(JavaPairRDD.fromJavaRDD(other.rdd().toJavaRDD()))));
		else return new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
				.join(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag()))));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions) {
		int pnum = (int) Math.ceil(Math.max(getNumPartitions(), other.getNumPartitions()) * ratioPartitions);
		// two stream, use 10 for devel testing.
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null == ssc) return new PairRDS<>(new WrappedRDD<>(pairRDD().join(JavaPairRDD.fromJavaRDD(other.rdd().toJavaRDD()), pnum)));
		else return new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
				.join(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag()), pnum)));

	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other) {
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null == ssc) return new PairRDS<>(new WrappedRDD<>(
				JavaPairRDD.fromJavaRDD(rdd().toJavaRDD()).leftOuterJoin(JavaPairRDD.fromJavaRDD(other.rdd().toJavaRDD()))));
		else return new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
				.leftOuterJoin(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag()))));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, float ratioPartitions) {
		int pnum = (int) Math.ceil(Math.max(getNumPartitions(), other.getNumPartitions()) * ratioPartitions);
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null == ssc) return new PairRDS<>(new WrappedRDD<>(
				JavaPairRDD.fromJavaRDD(rdd().toJavaRDD()).leftOuterJoin(JavaPairRDD.fromJavaRDD(other.rdd().toJavaRDD()), pnum)));
		else return new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(wrapped().dstream(ssc), k(), v())
				.leftOuterJoin(JavaPairDStream.fromPairDStream(other.wrapped().dstream(ssc), RDSupport.tag(), RDSupport.tag()), pnum)));
	}

	@Override
	@Deprecated
	public <U> PairRDS<K, Iterable<V>> groupByKey() {
		return isStream()
				? new PairRDS<>(new WrappedDStream<>(
						JavaPairDStream.fromPairDStream(((WrappedDStream<Tuple2<K, V>>) wrapped()).dstream(), k(), v()).groupByKey()))
				: new PairRDS<>(new WrappedRDD<>(pairRDD().groupByKey()));
	}

	@Override
	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		if (isStream()) {
			DStream<Tuple2<K, V>> ds = ((WrappedDStream<Tuple2<K, V>>) wrapped()).dstream();
			return new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(ds, k(), v()).reduceByKey(func)));
		} else {
			List<JavaPairRDD<K, V>> l = new ArrayList<>();
			foreachPairRDD(rdd -> l.add(rdd.reduceByKey(func)));
			return new PairRDS<>(new WrappedRDD<>(RDSupport.union(l)));
		}
	}

	@Override
	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions) {
		int pnum = (int) Math.ceil(getNumPartitions() * ratioPartitions);
		// two stream, use 10 for devel testing.
		int minpnum = pnum < 0 ? ((int) Math.ceil(ratioPartitions)) * 10 : pnum;
		if (isStream()) {
			DStream<Tuple2<K, V>> ds = ((WrappedDStream<Tuple2<K, V>>) wrapped()).dstream();
			return new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(ds, k(), v()).reduceByKey(func, minpnum)));
		} else {
			List<JavaPairRDD<K, V>> l = new ArrayList<>();
			foreachPairRDD(rdd -> l.add(rdd.reduceByKey(func, minpnum)));
			return new PairRDS<>(new WrappedRDD<>(RDSupport.union(l)));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public WrappedDataset<K, V> toDS(Class<V> vClass) {
		if (wrapped instanceof WrappedDataset) return (WrappedDataset<K, V>) wrapped;
		JavaRDD<Tuple2<K, V>> rdd = wrapped.jrdd();
		SQLContext ssc = new SQLContext(rdd.context());
		return new WrappedDataset<K, V>(ssc, rdd.map(t -> t._2));
	}

	@SuppressWarnings("unchecked")
	@Override
	@Deprecated
	public WrappedDataFrame<K, V> toDF(RowMarshaller marshaller) {
		if (wrapped instanceof WrappedDataFrame) return (WrappedDataFrame<K, V>) wrapped;
		JavaRDD<Tuple2<K, V>> rdd = wrapped.jrdd();
		return new WrappedDataFrame<K, V>(new SQLContext(rdd.context()), marshaller, rdd.map(t -> t._2));
	}
}
