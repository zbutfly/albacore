package net.butfly.albacore.calculus.factor.wrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.factor.rds.RDS;
import scala.Tuple2;
import scala.reflect.ClassTag;

public interface Wrapped<T> extends Serializable {
	public boolean isEmpty();

	public int getNumPartitions();

	public long count();

	public void foreachRDD(VoidFunction<JavaRDD<T>> consumer);

	public void foreach(VoidFunction<T> consumer);

	public T first();

	public default List<T> collect() {
		List<T> r = new ArrayList<>();
		foreach(r::add);
		return r;
	};

	public default ClassTag<T> classTag() {
		return RDS.tag();
	}

	public T reduce(Function2<T, T, T> func);

	public DStream<T> dstream(StreamingContext ssc);

	public RDD<T> rdd();

	public Collection<RDD<T>> rdds();

	public Wrapped<T> repartition(float ratio);

	public Wrapped<T> unpersist();

	public Wrapped<T> persist();

	public Wrapped<T> persist(StorageLevel level);

	public Wrapped<T> union(Wrapped<T> other);

	public Wrapped<T> filter(Function<T, Boolean> func);

	public <K2, V2> Wrapped<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func);

	public <T1> Wrapped<T1> map(Function<T, T1> func);

	public default <U> WDD<Tuple2<U, Iterable<T>>> groupBy(Function<T, U> func) {
		return new WDD<Tuple2<U, Iterable<T>>>(JavaRDD.fromRDD(rdd(), classTag()).groupBy(func::call));
	}

	public default <U> WDD<Tuple2<U, Iterable<T>>> groupBy(Function<T, U> func, int numPartitions) {
		return new WDD<Tuple2<U, Iterable<T>>>(JavaRDD.fromRDD(rdd(), classTag()).groupBy(func::call, numPartitions));
	}

	public <S> Wrapped<T> sortBy(Function<T, S> comp);

	Mode mode();

	Wrapped<T> wrapped();

	@SuppressWarnings("rawtypes")
	static StreamingContext ssc(Wrapped... wrapped) {
		for (Wrapped w : wrapped) {
			Wrapped ed = w.wrapped();
			if (ed.mode() == Mode.STREAMING) return ((WStream) ed).ssc;
		}
		return null;

	}
}
