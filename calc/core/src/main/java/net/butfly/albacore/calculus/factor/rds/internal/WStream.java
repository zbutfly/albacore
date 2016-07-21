package net.butfly.albacore.calculus.factor.rds.internal;

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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.Mode;
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
public class WStream<T> implements Wrapped<T> {
	private static final long serialVersionUID = 8010123164729388602L;
	final protected StreamingContext ssc;
	final transient DStream<T> dstream;

	protected WStream(DStream<T> dstream) {
		ssc = dstream.ssc();
		this.dstream = dstream;
	}

	public WStream(JavaDStreamLike<T, ?, ?> dstream) {
		this(dstream.dstream());
	}

	@Override
	public Mode mode() {
		return Mode.STREAMING;
	}

	@Override
	public int getNumPartitions() {
		return -1;
	}

	@Override
	public WStream<T> repartition(float ratio) {
		return new WStream<T>(JavaDStream.fromDStream(dstream, classTag())
				.transform((Function<JavaRDD<T>, JavaRDD<T>>) rdd -> rdd.repartition((int) Math.ceil(rdd.getNumPartitions() * ratio))));
	}

	@Override
	public WStream<T> unpersist() {
		return this;
	}

	@Override
	public WStream<T> persist() {
		return new WStream<T>(dstream.persist());
	}

	@Override
	public WStream<T> persist(StorageLevel level) {
		return new WStream<T>(dstream.persist(level));
	}

	@Override
	public final boolean isEmpty() {
		return false;
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
		if (WDD.class.isAssignableFrom(other.getClass()))
			return new WStream<T>(dstream.union(RDDDStream.stream(ssc, Mechanism.CONST, () -> other.jrdd()).dstream()));
		else if (WStream.class.isAssignableFrom(other.getClass())) return new WStream<T>(dstream.union(((WStream<T>) other).dstream));
		else throw new IllegalArgumentException();
	}

	@Override
	public WStream<T> filter(Function<T, Boolean> func) {
		return new WStream<T>(JavaDStream.fromDStream(dstream, classTag()).filter(func).dstream());
	}

	@Override
	public <K2, V2> WStream<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		return new WStream<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, classTag()).mapToPair(func));
	}

	@Override
	public final <T1> WStream<T1> map(Function<T, T1> func) {
		return new WStream<T1>(JavaDStream.fromDStream(dstream, classTag()).map(func).dstream());
	}

	@Override
	public <U> WDD<Tuple2<U, Iterable<T>>> groupBy(Function<T, U> func) {
		return new WDD<Tuple2<U, Iterable<T>>>(jrdd().groupBy(func));
	}

	@Override
	public <U> WDD<Tuple2<U, Iterable<T>>> groupBy(Function<T, U> func, int numPartitions) {
		return new WDD<Tuple2<U, Iterable<T>>>(jrdd().groupBy(func, numPartitions));
	}

	@Override
	public final T first() {
		Collection<RDD<T>> rdds = rdds();
		return rdds.isEmpty() ? null : rdds.iterator().next().first();
	}

	@Override
	public final T reduce(Function2<T, T, T> func) {
		List<T> rr = new ArrayList<>();
		JavaDStream.fromDStream(dstream, classTag()).reduce(func).foreachRDD(rdd -> {
			rr.add(rdd.first());
		});
		if (rr.isEmpty()) return null;
		return new WDD<T>(ssc.sc(), rr).reduce(func);
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
		return RDSupport.union(rdds());
	}

	@Override
	public Collection<RDD<T>> rdds() {
		List<RDD<T>> r = new ArrayList<>();
		foreachRDD((final JavaRDD<T> rdd) -> r.add(rdd.rdd()));
		return r;
	}

	@Override
	public <S> WDD<T> sortBy(Function<T, S> comp) {
		JavaRDD<T> rdd = jrdd();
		return new WDD<T>(rdd.sortBy(comp, true, rdd.getNumPartitions()));
	}

	@Override
	public Wrapped<T> wrapped() {
		return this;
	}
}
