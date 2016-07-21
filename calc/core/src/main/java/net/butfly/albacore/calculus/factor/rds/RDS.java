package net.butfly.albacore.calculus.factor.rds;

import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.factor.rds.internal.WDD;
import net.butfly.albacore.calculus.factor.rds.internal.Wrapped;
import scala.Tuple2;

/**
 * Single including any implementation of spark data.
 * 
 * @author zx
 *
 * @param <T>
 */
public class RDS<T> implements Wrapped<T> {
	private static final long serialVersionUID = -1898959212702322579L;
	protected Wrapped<T> wrapped;

	protected RDS(Wrapped<T> wrapper) {
		if (RDS.class.isAssignableFrom(wrapper.getClass())) throw new IllegalArgumentException();
		this.wrapped = wrapper;
	}

	protected RDS() {}

	@SafeVarargs
	public RDS(JavaSparkContext sc, T... t) {
		this.wrapped = new WDD<T>(sc.sc(), t);
	}

	@Override
	public Mode mode() {
		return wrapped().mode();
	}

	@Override
	public Wrapped<T> wrapped() {
		Wrapped<T> w = wrapped;
		while (RDS.class.isAssignableFrom(wrapped.getClass()))
			w = ((RDS<T>) w).wrapped;
		return w;
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<T>> consumer) {
		wrapped().foreachRDD(consumer);
	}

	@Override
	public void foreach(VoidFunction<T> consumer) {
		wrapped().foreach(consumer);
	}

	@Override
	public T reduce(Function2<T, T, T> func) {
		return wrapped().reduce(func);
	}

	@Override
	public DStream<T> dstream(StreamingContext ssc) {
		return wrapped().dstream(ssc);
	}

	@Override
	public RDD<T> rdd() {
		return wrapped().rdd();
	}

	@Override
	public Collection<RDD<T>> rdds() {
		return wrapped().rdds();
	}

	@Override
	public RDS<T> repartition(float ratio) {
		return new RDS<>(wrapped().repartition(ratio));
	}

	@Override
	public RDS<T> unpersist() {
		return new RDS<>(wrapped().unpersist());
	}

	@Override
	public RDS<T> persist() {
		return new RDS<>(wrapped().persist());
	}

	@Override
	public RDS<T> persist(StorageLevel level) {
		return new RDS<>(wrapped().persist(level));
	}

	@Override
	public RDS<T> union(Wrapped<T> other) {
		return new RDS<>(wrapped().union(other));
	}

	@Override
	public RDS<T> filter(Function<T, Boolean> func) {
		return new RDS<>(wrapped().filter(func));
	}

	@Override
	public <K2, V2> RDS<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		return new RDS<>(wrapped().mapToPair(func));
	}

	@Override
	public <T1> RDS<T1> map(Function<T, T1> func) {
		return new RDS<>(wrapped().map(func));
	}

	@Override
	public <S> RDS<T> sortBy(Function<T, S> comp) {
		return new RDS<>(wrapped().sortBy(comp));
	}

	@Override
	public T first() {
		return wrapped().first();
	}
}
