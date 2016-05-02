package net.butfly.albacore.calculus.factor.rds;

import java.util.Arrays;
import java.util.Collection;
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

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class WDD<T> implements Wrapped<T> {
	private static final long serialVersionUID = 3614737214835144193L;
	final private SparkContext sc;
	final protected transient RDD<T> rdd;

	protected WDD(RDD<T> rdd) {
		sc = rdd.context();
		this.rdd = rdd;
	}

	public WDD(JavaRDDLike<T, ?> rdd) {
		this(rdd.rdd());
	}

	@SafeVarargs
	public WDD(SparkContext sc, T... t) {
		this(sc, Arrays.asList(t));
	}

	public WDD(SparkContext sc, List<T> t) {
		this(sc.parallelize(JavaConversions.asScalaBuffer(t).seq(), sc.defaultParallelism(), RDSupport.tag()));
	}

	@Override
	public Mode mode() {
		return Mode.STOCKING;
	}

	@Override
	public int getNumPartitions() {
		return rdd.getNumPartitions();
	}

	@Override
	public WDD<T> repartition(float ratio) {
		return new WDD<T>(jrdd().repartition((int) Math.ceil(rdd.getNumPartitions() * ratio)).rdd());
	}

	@Override
	public WDD<T> unpersist() {
		return new WDD<T>(rdd.unpersist(true));
	}

	@Override
	public WDD<T> persist() {
		return new WDD<T>(rdd.persist());
	}

	@Override
	public WDD<T> persist(StorageLevel level) {
		return new WDD<T>(rdd.persist(level));
	}

	@Override
	public final boolean isEmpty() {
		return rdd.isEmpty();
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
		StreamingContext ssc;
		if (WDD.class.isAssignableFrom(other.getClass())) return new WDD<>(jrdd().union(other.jrdd()));
		else if (WStream.class.isAssignableFrom(other.getClass())) {
			ssc = ((WStream<T>) other).ssc;
			return new WStream<>(jdstream(ssc).union(other.jdstream(ssc)));
		} else throw new IllegalArgumentException();
	}

	@Override
	public WDD<T> filter(Function<T, Boolean> func) {
		return new WDD<T>(jrdd().filter(func).rdd());
	}

	@Override
	public <K2, V2> WDD<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		return new WDD<Tuple2<K2, V2>>(jrdd().mapToPair(func).rdd());
	}

	@Override
	public final <T1> WDD<T1> map(Function<T, T1> func) {
		return new WDD<T1>(jrdd().map(func).rdd());
	}

	@Override
	public final T first() {
		return rdd.first();
	}

	@Override
	public final T reduce(Function2<T, T, T> func) {
		Seq<T> seq = JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(func))).seq();
		return JavaRDD.fromRDD(sc.parallelize(seq, sc.defaultMinPartitions(), classTag()), classTag()).reduce(func);
	}

	@Override
	public final long count() {
		return rdd.count();
	}

	@Override
	public DStream<T> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> JavaRDD.fromRDD(rdd(), classTag())).dstream();
	};

	@Override
	public RDD<T> rdd() {
		return rdd;
	}

	@Override
	public Collection<RDD<T>> rdds() {
		return Arrays.asList(rdd);
	}

	@Override
	public <S> WDD<T> sortBy(Function<T, S> comp) {
		JavaRDD<T> rdd = JavaRDD.fromRDD(rdd(), classTag());
		return new WDD<T>(rdd.sortBy(comp, true, rdd.getNumPartitions()));
	}

	@Override
	public Wrapped<T> wrapped() {
		return this;
	}
}
