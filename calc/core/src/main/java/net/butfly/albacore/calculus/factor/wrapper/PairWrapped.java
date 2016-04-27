package net.butfly.albacore.calculus.factor.wrapper;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

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
import net.butfly.albacore.calculus.factor.rds.RDS;
import scala.Tuple2;
import scala.reflect.ClassTag;

public interface PairWrapped<K, V> extends Wrapped<Tuple2<K, V>> {
	public default ClassTag<V> v() {
		return RDS.tag();
	}

	public default ClassTag<K> k() {
		return RDS.tag();
	}

	public <RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, DataDetail<V> dd);

	public Map<K, V> collectAsMap();

	public Set<K> collectKeys();

	public void foreachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer);

	public void foreach(VoidFunction2<K, V> consumer);

	public <U> PairWrapper<K, Iterable<V>> groupByKey();

	public PairWrapper<K, V> reduceByKey(Function2<V, V, V> func);

	public PairWrapper<K, V> reduceByKey(Function2<V, V, V> func, int numPartitions);

	public <V2> PairWrapper<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other);

	public <V2> PairWrapper<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, int numPartitions);

	public <V2> PairWrapper<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other);

	public <V2> PairWrapper<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, int numPartitions);

	public JavaPairRDD<K, V> pairRDD();

	public Collection<JavaPairRDD<K, V>> pairRDDs();

	public PairWrapper<K, V> sortByKey(boolean asc);

	public <S> PairWrapper<K, V> sortBy(Function2<K, V, S> comp);

	public K maxKey();

	public K minKey();

	public PairWrapper<K, V> filter(Function2<K, V, Boolean> func);

	public PairWrapper<K, V> repartition(float ratio, boolean rehash);

	@Override
	public Tuple2<K, V> first();

	@Override
	public PairWrapper<K, V> filter(Function<Tuple2<K, V>, Boolean> func);

	@Override
	public PairWrapper<K, V> repartition(float ratio);

	@Override
	public PairWrapper<K, V> unpersist();

	@Override
	public PairWrapper<K, V> persist();

	@Override
	public PairWrapper<K, V> persist(StorageLevel level);

	@Override
	public PairWrapper<K, V> union(Wrapped<Tuple2<K, V>> other);

	@Override
	public <K2, V2> PairWrapper<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func);
}
