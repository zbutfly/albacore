package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.factor.rds.RDS;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class WrappedRDD<T> implements Wrapped<T> {
	private static final long serialVersionUID = 3614737214835144193L;
	final private SparkContext sc;
	final protected transient RDD<T> rdd;

	public WrappedRDD(RDD<T> rdd) {
		sc = rdd.context();
		this.rdd = rdd;
	}

	public WrappedRDD(JavaRDDLike<T, ?> rdd) {
		this(rdd.rdd());
	}

	@SafeVarargs
	public WrappedRDD(SparkContext sc, T... t) {
		this(sc, Arrays.asList(t));
	}

	public WrappedRDD(SparkContext sc, List<T> t) {
		this(sc.parallelize(JavaConversions.asScalaBuffer(t).seq(), sc.defaultParallelism(), RDSupport.tag()));
	}

	@Override
	public int getNumPartitions() {
		return rdd.getNumPartitions();
	}

	@Override
	public WrappedRDD<T> repartition(float ratio) {
		return new WrappedRDD<T>(jrdd().repartition((int) Math.ceil(rdd.getNumPartitions() * ratio)).rdd());
	}

	@Override
	public WrappedRDD<T> unpersist() {
		return new WrappedRDD<T>(rdd.unpersist(true));
	}

	@Override
	public WrappedRDD<T> persist(StorageLevel level) {
		if (null == level || StorageLevel.NONE().equals(level)) return this;
		return new WrappedRDD<T>(rdd.persist(level));
	}

	@Override
	public final boolean isEmpty() {
		return rdd.isEmpty();
	}

	@Override
	public T first() {
		return rdd.first();
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<T>> consumer) {
		try {
			consumer.call(jrdd());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void foreach(VoidFunction<T> consumer) {
		jrdd().foreach(consumer);
	}

	@Override
	public Wrapped<T> union(Wrapped<T> other) {
		if (RDS.class.isAssignableFrom(other.getClass())) return union(((RDS<T>) other).wrapped());
		if (WrappedRDD.class.isAssignableFrom(other.getClass())) return new WrappedRDD<>(jrdd().union(other.jrdd()));
		else if (WrappedDStream.class.isAssignableFrom(other.getClass())) return new WrappedDStream<>(jdstream(
				((WrappedDStream<T>) other).ssc).union(other.jdstream(((WrappedDStream<T>) other).ssc)));
		else throw new IllegalArgumentException();
	}

	@Override
	public WrappedRDD<T> filter(Function<T, Boolean> func) {
		return new WrappedRDD<T>(jrdd().filter(func).rdd());
	}

	@Override
	public <K2, V2> WrappedRDD<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func, Class<?>... vClass2) {
		return new WrappedRDD<Tuple2<K2, V2>>(jrdd().mapToPair(func).rdd());
	}

	@Override
	public final <T1> WrappedRDD<T1> map(Function<T, T1> func, Class<?>... vClass2) {
		return new WrappedRDD<T1>(jrdd().map(func).rdd());
	}

	@Override
	public final T reduce(Function2<T, T, T> func) {
		Seq<T> seq = JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(func))).seq();
		return sc.parallelize(seq, sc.defaultMinPartitions(), classTag()).toJavaRDD().reduce(func);
	}

	@Override
	public final long count() {
		return rdd.count();
	}

	@Override
	public DStream<T> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> jrdd()).dstream();
	};

	@Override
	public RDD<T> rdd() {
		return rdd;
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
}
