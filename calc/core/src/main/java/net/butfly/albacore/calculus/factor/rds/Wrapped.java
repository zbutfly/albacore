package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.lambda.Func2;
import net.butfly.albacore.calculus.lambda.PairFunc;
import net.butfly.albacore.calculus.lambda.VoidFunc;
import scala.Tuple2;

public interface Wrapped<T> {
	public boolean isEmpty();

	public int partitions();

	public long count();

	public void eachRDD(VoidFunc<RDD<T>> consumer);

	public void each(VoidFunc<T> consumer);

	public default List<T> collect() {
		List<T> r = new ArrayList<>();
		each(r::add);
		return r;
	};

	public T reduce(Func2<T, T, T> func);

	public RDD<T> rdd();

	public Collection<RDD<T>> rdds();

	public Wrapped<T> repartition(float ratio);

	public Wrapped<T> unpersist();

	public Wrapped<T> persist();

	public Wrapped<T> persist(StorageLevel level);

	public Wrapped<T> union(Wrapped<T> other);

	public Wrapped<T> filter(Func<T, Boolean> func);

	public <K2, V2> Wrapped<Tuple2<K2, V2>> mapToPair(PairFunc<T, K2, V2> func);

	public <T1> Wrapped<T1> map(Func<T, T1> func);

	public default <U> WrapRDD<Tuple2<U, Iterable<T>>> groupBy(Func<T, U> func) {
		return new WrapRDD<Tuple2<U, Iterable<T>>>(JavaRDD.fromRDD(rdd(), RDS.tag()).groupBy(func::call));
	}

	public default <U> WrapRDD<Tuple2<U, Iterable<T>>> groupBy(Func<T, U> func, int numPartitions) {
		return new WrapRDD<Tuple2<U, Iterable<T>>>(JavaRDD.fromRDD(rdd(), RDS.tag()).groupBy(func::call, numPartitions));
	}

	public <S> Wrapped<T> sortBy(Func<T, S> comp);
}
