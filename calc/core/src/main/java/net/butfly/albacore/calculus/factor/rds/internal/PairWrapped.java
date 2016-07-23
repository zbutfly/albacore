package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.Collection;
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

import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import scala.Tuple2;
import scala.reflect.ClassTag;

public interface PairWrapped<K, V> extends Wrapped<Tuple2<K, V>> {
	public default ClassTag<V> v() {
		return RDSupport.tag();
	}

	public default ClassTag<K> k() {
		return RDSupport.tag();
	}

	public <RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, DataDetail<V> dd);

	public Map<K, V> collectAsMap();

	public List<K> collectKeys();

	public void foreachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer);

	public void foreach(VoidFunction2<K, V> consumer);

	public <U> PairWrapped<K, Iterable<V>> groupByKey();

	public PairWrapped<K, V> reduceByKey(Function2<V, V, V> func);

	public PairWrapped<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions);

	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other);

	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions);

	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other);

	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, int numPartitions);

	public JavaPairRDD<K, V> pairRDD();

	public Collection<JavaPairRDD<K, V>> pairRDDs();

	public PairWrapped<K, V> sortByKey(boolean asc);

	public <S> PairWrapped<K, V> sortBy(Function2<K, V, S> comp);

	public K maxKey();

	public K minKey();

	public PairWrapped<K, V> filter(Function2<K, V, Boolean> func);

	public PairWrapped<K, V> repartition(float ratio, boolean rehash);

	@Override
	public Tuple2<K, V> first();

	@Override
	public PairWrapped<K, V> filter(Function<Tuple2<K, V>, Boolean> func);

	@Override
	public PairWrapped<K, V> repartition(float ratio);

	@Override
	public PairWrapped<K, V> unpersist();

	@Override
	public PairWrapped<K, V> persist();

	@Override
	public PairWrapped<K, V> persist(StorageLevel level);

	@Override
	public PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other);

	@Override
	public <K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func);
}
