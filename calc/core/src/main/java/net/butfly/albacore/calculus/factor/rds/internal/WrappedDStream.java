package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.ArrayList;
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
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import scala.Tuple2;

/**
 * Wrapper of DStream
 * 
 * @author butfly
 *
 * @param <T>
 */
public class WrappedDStream<T> implements Wrapped<T> {
	private static final long serialVersionUID = 8010123164729388602L;
	final protected StreamingContext ssc;
	final transient DStream<T> dstream;

	public WrappedDStream(DStream<T> dstream) {
		ssc = dstream.ssc();
		this.dstream = dstream;
	}

	public WrappedDStream(JavaDStreamLike<T, ?, ?> dstream) {
		this(dstream.dstream());
	}

	@Override
	public int getNumPartitions() {
		return 1;
	}

	@Override
	public WrappedDStream<T> repartition(float ratio) {
		return new WrappedDStream<T>(JavaDStream.fromDStream(dstream, classTag()).transform((Function<JavaRDD<T>, JavaRDD<T>>) rdd -> rdd
				.repartition((int) Math.ceil(rdd.getNumPartitions() * ratio))));
	}

	@Override
	public WrappedDStream<T> unpersist() {
		return this;
	}

	@Override
	public WrappedDStream<T> persist(StorageLevel level) {
		if (null == level || StorageLevel.NONE().equals(level)) return this;
		return new WrappedDStream<T>(dstream.persist(level));
	}

	@Override
	public final boolean isEmpty() {
		return false;
	}

	@Override
	public T first() {
		return null;
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<T>> consumer) {
		JavaDStream.fromDStream(dstream, classTag()).foreachRDD(rdd -> {
			consumer.call(rdd);
		});
	}

	@Override
	public void foreach(VoidFunction<T> consumer) {
		JavaDStream.fromDStream(dstream, classTag()).foreachRDD(rdd -> {
			rdd.foreach(consumer);
		});
	}

	@Override
	public Wrapped<T> union(Wrapped<T> other) {
		if (WrappedRDD.class.isAssignableFrom(other.getClass())) return new WrappedDStream<T>(dstream.union(RDDDStream.stream(ssc,
				Mechanism.CONST, () -> other.jrdd()).dstream()));
		else if (WrappedDStream.class.isAssignableFrom(other.getClass())) return new WrappedDStream<T>(dstream.union(
				((WrappedDStream<T>) other).dstream));
		else throw new IllegalArgumentException();
	}

	@Override
	public WrappedDStream<T> filter(Function<T, Boolean> func) {
		return new WrappedDStream<T>(JavaDStream.fromDStream(dstream, classTag()).filter(func).dstream());
	}

	@Override
	public <K2, V2> WrappedDStream<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func, Class<?>... vClass2) {
		return new WrappedDStream<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, classTag()).mapToPair(func));
	}

	@Override
	public final <T1> WrappedDStream<T1> map(Function<T, T1> func, Class<?>... vClass2) {
		return new WrappedDStream<T1>(JavaDStream.fromDStream(dstream, classTag()).map(func).dstream());
	}

	@Override
	public final T reduce(Function2<T, T, T> func) {
		List<T> rr = new ArrayList<>();
		JavaDStream.fromDStream(dstream, classTag()).reduce(func).foreachRDD(rdd -> {
			rr.add(rdd.first());
		});
		if (rr.isEmpty()) return null;
		return new WrappedRDD<T>(ssc.sc(), rr).reduce(func);
	}

	@Override
	public final long count() {
		long[] rr = new long[] { 0 };
		JavaDStream.fromDStream(dstream, classTag()).count().foreachRDD(rdd -> {
			rr[0] += rdd.first();
		});
		return rr[0];
	}

	@Override
	public DStream<T> dstream(StreamingContext ssc) {
		return dstream;
	};

	public DStream<T> dstream() {
		return dstream;
	};

	@Override
	public RDD<T> rdd() {
		List<RDD<T>> all = new ArrayList<>();
		foreachRDD(r -> all.add(r.rdd()));
		return RDSupport.union(all);
	}

	@Override
	public <S> WrappedRDD<T> sortBy(Function<T, S> comp, Class<?>... vClass2) {
		JavaRDD<T> rdd = jrdd();
		return new WrappedRDD<T>(rdd.sortBy(comp, true, rdd.getNumPartitions()));
	}

	@Override
	public Wrapped<T> wrapped() {
		return this;
	}

	@Override
	public boolean isStream() {
		return true;
	}

	@Override
	public List<T> collect() {
		List<T> l = new ArrayList<>();
		foreach(t -> l.add(t));
		return l;
	}
}
