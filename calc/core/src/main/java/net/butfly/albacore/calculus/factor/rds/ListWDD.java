package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.collection.JavaConversions;

@Deprecated
public class ListWDD<T> implements Wrapped<T> {
	private static final long serialVersionUID = 3614737214835144193L;
	final private SparkContext sc;
	final protected transient List<RDD<T>> rdds;

	protected ListWDD(Collection<RDD<T>> rdds) {
		sc = rdds.isEmpty() ? null : rdds.iterator().next().context();
		this.rdds = new ArrayList<>(rdds);
	}

	@SafeVarargs
	protected ListWDD(RDD<T>... rdds) {
		this(Arrays.asList(rdds));
	}

	@SafeVarargs
	public ListWDD(JavaRDDLike<T, ?>... rdd) {
		this(Reflections.transform(Arrays.asList(rdd), r -> r.rdd()));
	}

	@SafeVarargs
	public ListWDD(SparkContext sc, T... t) {
		this(sc, Arrays.asList(t));
	}

	public ListWDD(SparkContext sc, List<T> t) {
		this(sc.parallelize(JavaConversions.asScalaBuffer(t).seq(), sc.defaultParallelism(), RDSupport.tag()));
	}

	@Override
	public Mode mode() {
		return Mode.STOCKING;
	}

	@Override
	public int getNumPartitions() {
		int p = 0;
		for (RDD<T> rdd : rdds)
			p += rdd.getNumPartitions();
		return p;
	}

	@Override
	public ListWDD<T> repartition(float ratio) {
		return new ListWDD<T>(Reflections.transform(rdds,
				rdd -> JavaRDD.fromRDD(rdd, classTag()).repartition((int) Math.ceil(rdd.getNumPartitions() * ratio)).rdd()));
	}

	@Override
	public ListWDD<T> unpersist() {
		return new ListWDD<T>(Reflections.transform(rdds, v -> v.unpersist(true)));
	}

	@Override
	public ListWDD<T> persist() {
		return new ListWDD<T>(Reflections.transform(rdds, RDD<T>::persist));
	}

	@Override
	public ListWDD<T> persist(StorageLevel level) {
		return new ListWDD<T>(Reflections.transform(rdds, v -> v.persist(level)));
	}

	@Override
	public final boolean isEmpty() {
		for (RDD<T> rdd : rdds)
			if (!rdd.isEmpty()) return false;
		return true;
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<T>> consumer) {
		for (RDD<T> rdd : rdds)
			try {
				consumer.call(JavaRDD.fromRDD(rdd, classTag()));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
	}

	@Override
	public void foreach(VoidFunction<T> consumer) {
		for (RDD<T> rdd : rdds)
			JavaRDD.fromRDD(rdd, classTag()).foreach(consumer);;
	}

	@Override
	public Wrapped<T> union(Wrapped<T> other) {
		if (RDS.class.isAssignableFrom(other.getClass())) return union(((RDS<T>) other).wrapped);
		List<RDD<T>> nrdds = new ArrayList<>(rdds);
		if (ListWDD.class.isAssignableFrom(other.getClass())) nrdds.addAll(((ListWDD<T>) other).rdds);
		else if (WStream.class.isAssignableFrom(other.getClass()))
			JavaDStream.fromDStream(((WStream<T>) other).dstream, classTag()).transform(r -> {
				nrdds.add(r.rdd());
				return null;
			});
		else throw new IllegalArgumentException();
		return new ListWDD<T>(nrdds);

	}

	@Override
	public ListWDD<T> filter(Function<T, Boolean> func) {
		return new ListWDD<T>(Reflections.transform(rdds, r -> JavaRDD.fromRDD(r, classTag()).filter(func).rdd()));
	}

	@Override
	public <K2, V2> ListWDD<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		return new ListWDD<Tuple2<K2, V2>>(
				Reflections.transform(rdds, (RDD<T> rdd) -> JavaRDD.fromRDD(rdd, classTag()).mapToPair(func).rdd()));
	}

	@Override
	public final <T1> ListWDD<T1> map(Function<T, T1> func) {
		return new ListWDD<T1>(Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, classTag()).map(func).rdd()));
	}

	@Override
	public final T first() {
		return rdds.isEmpty() ? null : rdds.get(0).first();
	}

	@Override
	public final T reduce(Function2<T, T, T> func) {
		return JavaRDD.fromRDD(sc.parallelize(
				JavaConversions.asScalaBuffer(Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, classTag()).reduce(func))).seq(),
				sc.defaultMinPartitions(), classTag()), classTag()).reduce(func);
	}

	@Override
	public final long count() {
		long r = 0;
		for (RDD<T> rdd : rdds)
			r += rdd.count();
		return r;
	}

	@Override
	public DStream<T> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> JavaRDD.fromRDD(rdd(), classTag())).dstream();
	};

	@Override
	public RDD<T> rdd() {
		return RDSupport.union(rdds);
	}

	@Override
	public Collection<RDD<T>> rdds() {
		return rdds;
	}

	@Override
	public <S> ListWDD<T> sortBy(Function<T, S> comp) {
		JavaRDD<T> rdd = JavaRDD.fromRDD(rdd(), classTag());
		return new ListWDD<T>(rdd.sortBy(comp, true, rdd.getNumPartitions()));
	}

	@Override
	public Wrapped<T> wrapped() {
		return this;
	}
}
