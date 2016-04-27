package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;

import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.lambda.Func2;
import net.butfly.albacore.calculus.lambda.PairFunc;
import net.butfly.albacore.calculus.lambda.VoidFunc;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.collection.JavaConversions;

public class WrapRDD<T> implements Wrapped<T> {
	final private SparkContext sc;
	final protected transient List<RDD<T>> rdds;

	protected WrapRDD(Collection<RDD<T>> rdds) {
		sc = rdds.isEmpty() ? null : rdds.iterator().next().context();
		this.rdds = new ArrayList<>(rdds);
	}

	@SafeVarargs
	protected WrapRDD(RDD<T>... rdds) {
		this(Arrays.asList(rdds));
	}

	@SafeVarargs
	public WrapRDD(JavaRDDLike<T, ?>... rdd) {
		this(Reflections.transform(Arrays.asList(rdd), r -> r.rdd()));
	}

	@SafeVarargs
	public WrapRDD(SparkContext sc, T... t) {
		this(sc, Arrays.asList(t));
	}

	public WrapRDD(SparkContext sc, List<T> t) {
		this(sc.parallelize(JavaConversions.asScalaBuffer(t).seq(), sc.defaultParallelism(), RDS.tag()));
	}

	@Override
	public int partitions() {
		int p = 0;
		for (RDD<T> rdd : rdds)
			p += rdd.getNumPartitions();
		return p;
	}

	@Override
	public WrapRDD<T> repartition(float ratio) {
		return new WrapRDD<T>(Reflections.transform(rdds,
				rdd -> JavaRDD.fromRDD(rdd, RDS.tag()).repartition((int) Math.ceil(rdd.getNumPartitions() * ratio)).rdd()));
	}

	@Override
	public WrapRDD<T> unpersist() {
		return new WrapRDD<T>(Reflections.transform(rdds, v -> v.unpersist(true)));
	}

	@Override
	public WrapRDD<T> persist() {
		return new WrapRDD<T>(Reflections.transform(rdds, RDD<T>::persist));
	}

	@Override
	public WrapRDD<T> persist(StorageLevel level) {
		return new WrapRDD<T>(Reflections.transform(rdds, v -> v.persist(level)));
	}

	@Override
	public final boolean isEmpty() {
		for (RDD<T> rdd : rdds)
			if (!rdd.isEmpty()) return false;
		return true;
	}

	@Override
	public void eachRDD(VoidFunc<RDD<T>> consumer) {
		for (RDD<T> rdd : rdds)
			consumer.call(rdd);
	}

	@Override
	public void each(VoidFunc<T> consumer) {
		for (RDD<T> rdd : rdds)
			JavaRDD.fromRDD(rdd, RDS.tag()).foreach(consumer::call);;
	}

	@Override
	public Wrapped<T> union(Wrapped<T> other) {
		List<RDD<T>> nrdds = new ArrayList<>(rdds);
		if (WrapRDD.class.isAssignableFrom(other.getClass())) nrdds.addAll(((WrapRDD<T>) other).rdds);
		else if (WrapDStream.class.isAssignableFrom(other.getClass()))
			JavaDStream.fromDStream(((WrapDStream<T>) other).dstream, RDS.tag()).transform(r -> {
				nrdds.add(r.rdd());
				return null;
			});
		else throw new IllegalArgumentException();
		return new WrapRDD<T>(nrdds);

	}

	@Override
	public WrapRDD<T> filter(Func<T, Boolean> func) {
		return new WrapRDD<T>(Reflections.transform(rdds, r -> JavaRDD.fromRDD(r, RDS.tag()).filter(func::call).rdd()));
	}

	@Override
	public <K2, V2> WrapRDD<Tuple2<K2, V2>> mapToPair(PairFunc<T, K2, V2> func) {
		return new WrapRDD<Tuple2<K2, V2>>(
				Reflections.transform(rdds, (RDD<T> rdd) -> JavaRDD.fromRDD(rdd, RDS.tag()).mapToPair(func::call).rdd()));
	}

	@Override
	public final <T1> WrapRDD<T1> map(Func<T, T1> func) {
		return new WrapRDD<T1>(Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, RDS.tag()).map(func::call).rdd()));
	}

	@Override
	public final T reduce(Func2<T, T, T> func) {
		return JavaRDD.fromRDD(sc.parallelize(
				JavaConversions.asScalaBuffer(Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, RDS.tag()).reduce(func::call))).seq(),
				sc.defaultMinPartitions(), RDS.tag()), RDS.tag()).reduce(func::call);
	}

	@Override
	public final long count() {
		long r = 0;
		for (RDD<T> rdd : rdds)
			r += rdd.count();
		return r;
	}

	@Override
	public RDD<T> rdd() {
		return RDS.union(rdds);
	}

	@Override
	public Collection<RDD<T>> rdds() {
		return rdds;
	}

	@Override
	public <S> WrapRDD<T> sortBy(Func<T, S> comp) {
		RDD<T> rdd = rdd();
		return new WrapRDD<T>(JavaRDD.fromRDD(rdd, RDS.tag()).sortBy(comp::call, true, rdd.getNumPartitions()));
	}
}
