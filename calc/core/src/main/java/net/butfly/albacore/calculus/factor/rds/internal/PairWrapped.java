package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;

import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import scala.Tuple2;
import scala.reflect.ClassTag;

public interface PairWrapped<K, V> extends Wrapped<Tuple2<K, V>> {
	default ClassTag<V> v() {
		return RDSupport.tag();
	}

	default ClassTag<K> k() {
		return RDSupport.tag();
	}

	<RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, DataDetail<V> dd);

	Map<K, V> collectAsMap();

	List<K> collectKeys();

	void foreachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer);

	void foreach(VoidFunction2<K, V> consumer);

	@Deprecated
	<U> PairWrapped<K, Iterable<V>> groupByKey();

	PairWrapped<K, V> reduceByKey(Function2<V, V, V> func);

	PairWrapped<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions);

	<V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other);

	<V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions);

	<V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other);

	<V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, float ratioPartitions);

	default JavaPairRDD<K, V> pairRDD() {
		return JavaPairRDD.fromJavaRDD(rdd().toJavaRDD());
	};

	PairWrapped<K, V> sortByKey(boolean asc);

	<S> PairWrapped<K, V> sortBy(Function2<K, V, S> comp);

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

	PairWrapped<K, V> repartition(float ratio, boolean rehash);

	@Override
	PairWrapped<K, V> filter(Function<Tuple2<K, V>, Boolean> func);

	@Override
	PairWrapped<K, V> repartition(float ratio);

	@Override
	PairWrapped<K, V> unpersist();

	@Override
	PairWrapped<K, V> persist();

	@Override
	PairWrapped<K, V> persist(StorageLevel level);

	@Override
	PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other);

	@Override
	<K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func);

	WrappedDataset<K, V> toDS(Class<V> vClass);

	@Deprecated
	WrappedDataFrame<K, V> toDF(RowMarshaller marshaller);
}
