package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.InputDStream;

import net.butfly.albacore.calculus.factor.rds.RDS;
import net.butfly.albacore.calculus.lambda.Func0;
import net.butfly.albacore.calculus.lambda.Func2;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;

public abstract class RDDDStream<T> extends InputDStream<T> {
	public enum Mechanism {
		CONST, STOCK, FRESH, @Deprecated BATCH, @Deprecated CACHE
	}

	protected SparkContext sc;
	protected RDD<T> current;

	RDDDStream(StreamingContext ssc) {
		super(ssc, RDS.tag());
		this.sc = ssc.sc();
	}

	abstract protected RDD<T> load();

	public Option<RDD<T>> compute(Time time) {
		trace(() -> "RDD [" + name() + "] inputted as streaming with count: " + current.count());
		return Option.apply(current);
	}

	@Override
	final public String name() {
		if (current == null) return "[UNINITED]";
		String n = current.name();
		if (n != null) return n;
		n = current.toDebugString();
		if (n != null) return n;
		n = super.name();
		if (n != null) return n;
		return current.getClass().toString();
	}

	@Override
	final public void start() {}

	@Override
	final public void stop() {}

	final protected void trace(Func0<String> msg) {
		if (log().isTraceEnabled()) log().trace(msg.call());
	}

	public static <K, V> JavaPairDStream<K, V> pstream(StreamingContext ssc, Mechanism mechanism, Func0<JavaPairRDD<K, V>> rdd) {
		try {
			return JavaPairDStream.fromPairDStream(new RDDInputDStream<Tuple2<K, V>>(ssc, mechanism, () -> rdd.call().map(t -> t).rdd()),
					RDS.tag(), RDS.tag());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Deprecated
	public static <K, V> JavaPairDStream<K, V> bpstream(StreamingContext ssc, long batch, Func2<Long, K, JavaPairRDD<K, V>> batcher,
			Comparator<K> comparator) {
		try {
			return JavaPairDStream.fromPairDStream(
					new RDDBatchInputDStream<K, V>(ssc, batch, (limit, offset) -> batcher.call(limit, offset).rdd(), comparator), RDS.tag(),
					RDS.tag());
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static <R> JavaDStream<R> stream(StreamingContext ssc, Mechanism mechanism, Func0<JavaRDD<R>> rdd) {
		return JavaDStream.fromDStream(new RDDInputDStream<R>(ssc, mechanism, () -> rdd.call().rdd()), RDS.tag());
	}

	@SafeVarargs
	public static <R> JavaRDD<R> rddValue(SparkContext sc, R... r) {
		return JavaRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(Arrays.asList(r)).seq(), sc.defaultParallelism(), RDS.tag()),
				RDS.tag());
	}

	public static <R> JavaRDD<R> rddList(SparkContext sc, List<R> rs) {
		return JavaRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(rs).seq(), sc.defaultParallelism(), RDS.tag()), RDS.tag());
	}

	@SuppressWarnings("unchecked")
	public static <R> JavaDStream<R> streamValue(StreamingContext ssc, R... r) {
		JavaRDD<R> rdd = rddValue(ssc.sc(), r);
		return stream(ssc, Mechanism.CONST, () -> rdd);
	}

	public static <R> JavaDStream<R> streamList(StreamingContext ssc, List<R> rs) {
		JavaRDD<R> rdd = rddList(ssc.sc(), rs);
		return stream(ssc, Mechanism.CONST, () -> rdd);
	}
}