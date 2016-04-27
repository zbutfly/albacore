package net.butfly.albacore.calculus.factor.wrapper;

import java.util.Collection;

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
import scala.Tuple2;

public class Wrapper<T> implements Wrapped<T> {
	private static final long serialVersionUID = -1898959212702322579L;
	protected Wrapped<T> wrapped;

	protected Wrapper(Wrapped<T> wrapper) {
		if (Wrapper.class.isAssignableFrom(wrapper.getClass())) throw new IllegalArgumentException();
		this.wrapped = wrapper;
	}

	protected Wrapper() {}

	@Override
	public Mode mode() {
		return wrapped().mode();
	}

	@Override
	public Wrapped<T> wrapped() {
		Wrapped<T> w = wrapped;
		while (Wrapper.class.isAssignableFrom(wrapped.getClass()))
			w = ((Wrapper<T>) w).wrapped;
		return w;
	}

	@Override
	public boolean isEmpty() {
		return wrapped().isEmpty();
	}

	@Override
	public int getNumPartitions() {
		return wrapped().getNumPartitions();
	}

	@Override
	public long count() {
		return wrapped().count();
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
	public Wrapper<T> repartition(float ratio) {
		return new Wrapper<>(wrapped().repartition(ratio));
	}

	@Override
	public Wrapper<T> unpersist() {
		return new Wrapper<>(wrapped().unpersist());
	}

	@Override
	public Wrapper<T> persist() {
		return new Wrapper<>(wrapped().persist());
	}

	@Override
	public Wrapper<T> persist(StorageLevel level) {
		return new Wrapper<>(wrapped().persist(level));
	}

	@Override
	public Wrapper<T> union(Wrapped<T> other) {
		return new Wrapper<>(wrapped().union(other));
	}

	@Override
	public Wrapper<T> filter(Function<T, Boolean> func) {
		return new Wrapper<>(wrapped().filter(func));
	}

	@Override
	public <K2, V2> Wrapper<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		return new Wrapper<>(wrapped().mapToPair(func));
	}

	@Override
	public <T1> Wrapper<T1> map(Function<T, T1> func) {
		return new Wrapper<>(wrapped().map(func));
	}

	@Override
	public <S> Wrapper<T> sortBy(Function<T, S> comp) {
		return new Wrapper<>(wrapped().sortBy(comp));
	}

	@Override
	public T first() {
		return wrapped().first();
	}
}
