package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.lambda.Func2;
import net.butfly.albacore.calculus.lambda.PairFunc;
import net.butfly.albacore.calculus.lambda.VoidFunc;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import scala.Tuple2;

public class WrapDStream<T> implements Wrapped<T> {
	final private StreamingContext ssc;
	final transient DStream<T> dstream;

	protected WrapDStream(DStream<T> dstream) {
		ssc = dstream.ssc();
		this.dstream = dstream;
	}

	public WrapDStream(JavaDStreamLike<T, ?, ?> dstream) {
		this(dstream.dstream());
	}

	@Override
	public int partitions() {
		return -1;
	}

	@Override
	public WrapDStream<T> repartition(float ratio) {
		return new WrapDStream<T>(JavaDStream.fromDStream(dstream, RDS.tag())
				.transform((Function<JavaRDD<T>, JavaRDD<T>>) rdd -> rdd.repartition((int) Math.ceil(rdd.partitions().size() * ratio))));
	}

	@Override
	public WrapDStream<T> unpersist() {
		return this;
	}

	@Override
	public WrapDStream<T> persist() {
		return new WrapDStream<T>(dstream.persist());
	}

	@Override
	public WrapDStream<T> persist(StorageLevel level) {
		return new WrapDStream<T>(dstream.persist(level));
	}

	@Override
	public final boolean isEmpty() {
		return false;
	}

	@Override
	public void eachRDD(VoidFunc<RDD<T>> consumer) {
		JavaDStream.fromDStream(dstream, RDS.tag()).foreachRDD(rdd -> consumer.call(rdd.rdd()));
	}

	@Override
	public void each(VoidFunc<T> consumer) {
		JavaDStream.fromDStream(dstream, RDS.tag()).foreachRDD(rdd -> rdd.foreach(consumer::call));
	}

	@Override
	public Wrapped<T> union(Wrapped<T> other) {
		if (WrapRDD.class.isAssignableFrom(other.getClass())) return new WrapDStream<T>(
				dstream.union(RDDDStream.stream(dstream.ssc(), Mechanism.CONST, () -> JavaRDD.fromRDD(other.rdd(), RDS.tag())).dstream()));
		else if (WrapDStream.class.isAssignableFrom(other.getClass()))
			return new WrapDStream<T>(dstream.union(((WrapDStream<T>) other).dstream));
		else throw new IllegalArgumentException();
	}

	@Override
	public WrapDStream<T> filter(Func<T, Boolean> func) {
		return new WrapDStream<T>(JavaDStream.fromDStream(dstream, RDS.tag()).filter(func::call).dstream());
	}

	@Override
	public <K2, V2> WrapDStream<Tuple2<K2, V2>> mapToPair(PairFunc<T, K2, V2> func) {
		return new WrapDStream<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, RDS.tag()).mapToPair(func::call));
	}

	@Override
	public final <T1> WrapDStream<T1> map(Func<T, T1> func) {
		return new WrapDStream<T1>(JavaDStream.fromDStream(dstream, RDS.tag()).map(func::call).dstream());
	}

	@Override
	public <U> WrapRDD<Tuple2<U, Iterable<T>>> groupBy(Func<T, U> func) {
		return new WrapRDD<Tuple2<U, Iterable<T>>>(JavaRDD.fromRDD(rdd(), RDS.tag()).groupBy(func::call));
	}

	@Override
	public <U> WrapRDD<Tuple2<U, Iterable<T>>> groupBy(Func<T, U> func, int numPartitions) {
		return new WrapRDD<Tuple2<U, Iterable<T>>>(JavaRDD.fromRDD(rdd(), RDS.tag()).groupBy(func::call, numPartitions));
	}

	@Override
	public final T reduce(Func2<T, T, T> func) {
		List<T> rr = new ArrayList<>();
		JavaDStream.fromDStream(dstream, RDS.tag()).reduce(func::call).foreachRDD(rdd -> {
			rr.add(rdd.first());
		});
		if (rr.isEmpty()) return null;
		return new WrapRDD<T>(ssc.sc(), rr).reduce(func);
	}

	@Override
	public final long count() {
		long[] rr = new long[] { 0 };
		JavaDStream.fromDStream(dstream, RDS.tag()).count().foreachRDD(rdd -> {
			rr[0] += rdd.first();
		});
		return rr[0];
	}

	@Override
	public RDD<T> rdd() {
		return RDS.union(rdds());
	}

	@Override
	public Collection<RDD<T>> rdds() {
		List<RDD<T>> r = new ArrayList<>();
		eachRDD(r::add);
		return r;
	}

	@Override
	public <S> WrapRDD<T> sortBy(Func<T, S> comp) {
		JavaRDD<T> rdd = JavaRDD.fromRDD(rdd(), RDS.tag());
		return new WrapRDD<T>(rdd.sortBy(comp::call, true, rdd.partitions().size()));
	}
}
