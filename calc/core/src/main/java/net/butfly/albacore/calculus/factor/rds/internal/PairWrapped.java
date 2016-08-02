package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import scala.Tuple2;
import scala.reflect.ClassTag;

public interface PairWrapped<K, V> extends Wrapped<Tuple2<K, V>> {
	default ClassTag<V> v() {
		return RDSupport.tag();
	}

	default ClassTag<K> k() {
		return RDSupport.tag();
	}

	default <RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, FactroingConfig<V> dd) {
		foreachRDD(r -> ds.save(r.mapToPair(t -> (Tuple2<WK, WV>) ds.beforeWriting(t._1, t._2)).filter(t -> t != null && t._1 != null
				&& t._2 != null), dd));
	}

	default Map<K, V> collectAsMap() {
		Map<K, V> r = new HashMap<>();
		foreach((k, v) -> r.put(k, v));
		return r;
	}

	default List<K> collectKeys(Class<K> kClass) {
		return map(t -> t._1, kClass).collect();
	}

	default void foreach(VoidFunction2<K, V> consumer) {
		jrdd().foreach(t -> consumer.call(t._1, t._2));
	};

	@Deprecated
	<U> PairWrapped<K, Iterable<V>> groupByKey();

	PairWrapped<K, V> reduceByKey(Function2<V, V, V> func);

	PairWrapped<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions);

	<V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2);

	<V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions, Class<?>... vClass2);

	<V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2);

	<V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, float ratioPartitions, Class<?>... vClass2);

	default JavaPairDStream<K, V> pairStream(StreamingContext ssc) {
		if (isStream()) return JavaPairDStream.fromPairDStream(((WrappedDStream<Tuple2<K, V>>) wrapped()).dstream, k(), v());
		else return RDDDStream.pstream(ssc, Mechanism.CONST, () -> this);
	};

	default JavaPairRDD<K, V> pairRDD() {
		return JavaPairRDD.fromJavaRDD(jrdd());
	};

	PairWrapped<K, V> sortByKey(boolean asc);

	<S> PairWrapped<K, V> sortBy(Function2<K, V, S> comp, Class<?>... cls);

	default K maxKey() {
		@SuppressWarnings("unchecked")
		Ordering<K> c = (Ordering<K>) Ordering.natural();
		return reduce((t1, t2) -> (c.compare(t1._1, t2._1) > 0 ? t1 : t2))._1;
	}

	default K minKey() {
		@SuppressWarnings("unchecked")
		Ordering<K> c = (Ordering<K>) Ordering.natural();
		return reduce((t1, t2) -> (c.compare(t1._1, t2._1) < 0 ? t1 : t2))._1;
	};

	PairWrapped<K, V> filter(Function2<K, V, Boolean> func);

	default PairWrapped<K, V> repartition(float ratio, boolean rehash) {
		if (!rehash) return repartition(ratio);
		Partitioner p = new HashPartitioner((int) Math.ceil(getNumPartitions() * ratio));
		return isStream() ? new PairRDS<>(new WrappedDStream<>(JavaPairDStream.fromPairDStream(dstream(Wrapped.streaming(this)), k(), v())
				.transformToPair((Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>>) rdd -> rdd.partitionBy(p))))
				: new PairRDS<>(new WrappedRDD<>(JavaPairRDD.fromJavaRDD(rdd().toJavaRDD()).partitionBy(p)));

	};

	@Override
	PairWrapped<K, V> filter(Function<Tuple2<K, V>, Boolean> func);

	@Override
	PairWrapped<K, V> repartition(float ratio);

	@Override
	PairWrapped<K, V> unpersist();

	@Override
	default PairWrapped<K, V> persist(StorageLevel level) {
		if (null == level || StorageLevel.NONE().equals(level)) return this;
		else return new PairRDS<>(wrapped().persist(level));
	}

	@Override
	PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other);

	@Override
	<K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func, Class<?>... vClass2);

	WrappedDataset<K, V> toDS(Class<?>... vClass2);

	@Deprecated
	WrappedDataFrame<K, V> toDF(RowMarshaller marshaller);
}
