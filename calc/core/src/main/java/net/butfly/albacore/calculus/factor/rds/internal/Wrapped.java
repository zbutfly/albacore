package net.butfly.albacore.calculus.factor.rds.internal;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Logable;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Root of wrapper of spark data
 * 
 * @author zx
 *
 * @param <T>
 */
public interface Wrapped<T> extends Serializable, Logable {
	default boolean isStream() {
		return false;
	}

	default boolean isEmpty() {
		return wrapped().isEmpty();
	}

	default int getNumPartitions() {
		return wrapped().getNumPartitions();
	}

	default long count() {
		return jrdd().count();
	}

	default void foreachRDD(VoidFunction<JavaRDD<T>> consumer) {
		wrapped().foreachRDD(consumer);
	}

	default void foreach(VoidFunction<T> consumer) {
		wrapped().foreach(consumer);
	};

	default T first() {
		return wrapped().first();
	}

	default List<T> collect() {
		return wrapped().collect();
	};

	default ClassTag<T> classTag() {
		return RDSupport.tag();
	}

	default T reduce(Function2<T, T, T> func) {
		return wrapped().reduce(func);
	};

	DStream<T> dstream(StreamingContext ssc);

	default RDD<T> rdd() {
		return wrapped().rdd();
	};

	Wrapped<T> repartition(float ratio);

	Wrapped<T> unpersist();

	Wrapped<T> persist(StorageLevel level);

	Wrapped<T> union(Wrapped<T> other);

	Wrapped<T> filter(Function<T, Boolean> func);

	<K2, V2> Wrapped<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func, Class<?>... vClass2);

	<T1> Wrapped<T1> map(Function<T, T1> func, Class<?>... tClass1);

	@Deprecated
	default <U> WrappedRDD<Tuple2<U, Iterable<T>>> groupBy(Function<T, U> func) {
		return new WrappedRDD<Tuple2<U, Iterable<T>>>(jrdd().groupBy(func));
	}

	@Deprecated
	default <U> WrappedRDD<Tuple2<U, Iterable<T>>> groupBy(Function<T, U> func, int numPartitions) {
		return new WrappedRDD<Tuple2<U, Iterable<T>>>(jrdd().groupBy(func, numPartitions));
	}

	<S> Wrapped<T> sortBy(Function<T, S> comp, Class<?>... cls);

	Wrapped<T> wrapped();

	static StreamingContext streaming(Wrapped<?>... wrapped) {
		for (Wrapped<?> w : wrapped)
			if (w.isStream()) return ((WrappedDStream<?>) w.wrapped()).ssc;
		return null;
	}

	default JavaDStream<T> stream(StreamingContext ssc) {
		if (isStream()) return JavaDStream.fromDStream(((WrappedDStream<T>) wrapped()).dstream, classTag());
		else return RDDDStream.stream(ssc, Mechanism.CONST, () -> jrdd());
	}

	default JavaRDD<T> jrdd() {
		return rdd().toJavaRDD();
	}

	default JavaDStream<T> jdstream(StreamingContext ssc) {
		return JavaDStream.fromDStream(dstream(ssc), classTag());
	}
}
