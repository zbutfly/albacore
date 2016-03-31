package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;

import scala.Tuple2;
import scala.runtime.BoxedUnit;

public class PairRDS<K, V> extends RDS<Tuple2<K, V>> {
	private static final long serialVersionUID = 2213547213519725138L;

	protected PairRDS() {}

	protected PairRDS(DStream<Tuple2<K, V>> dstream) {
		super(dstream);
	}

	@SafeVarargs
	protected PairRDS(RDD<Tuple2<K, V>>... rdds) {
		super(rdds);
	}

	@SafeVarargs
	public PairRDS(JavaRDDLike<Tuple2<K, V>, ?>... rdd) {
		super(rdd);
	}

	public PairRDS(JavaDStreamLike<Tuple2<K, V>, ?, ?> dstream) {
		super(dstream);
	}

	@SafeVarargs
	public PairRDS(JavaSparkContext sc, Tuple2<K, V>... t) {
		super(sc, t);
	}

	public PairRDS(JavaSparkContext sc, Map<K, V> map) {
		this(sc.parallelize(new ArrayList<>(map.entrySet())).map(e -> new Tuple2<>(e.getKey(), e.getValue())));
	}

	public void eachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<Tuple2<K, V>> rdd : rdds)
				try {
					consumer.call(JavaPairRDD.fromRDD(rdd, tag(), tag()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			break;
		case DSTREAM:
			dstream.foreachRDD(wrap(rdd -> {
				consumer.call(JavaPairRDD.fromRDD(rdd, tag(), tag()));
				return BoxedUnit.UNIT;
			}));
			break;
		}
	}

	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		switch (type) {
		case RDD:
			return new PairRDS<K, V>(JavaPairRDD.fromRDD(union(rdds), tag(), tag()).reduceByKey(func));
		case DSTREAM:
			return new PairRDS<K, V>(JavaPairDStream.fromPairDStream(dstream, tag(), tag()).reduceByKey(func));
		default:
			throw new IllegalArgumentException();
		}
	}

	public <W> PairRDS<K, Tuple2<V, W>> join(PairRDS<K, W> other) {
		switch (type) {
		case RDD:
			JavaPairRDD<K, V> r1 = JavaPairRDD.fromRDD(union(rdds), tag(), tag());
			JavaPairRDD<K, W> r2 = JavaPairRDD.fromRDD(union(other.rdds), tag(), tag());
			return new PairRDS<K, Tuple2<V, W>>(r1.join(r2));
		case DSTREAM:
			JavaPairDStream<K, V> s1 = JavaPairDStream.fromPairDStream(dstream, tag(), tag());
			JavaPairDStream<K, W> s2 = JavaPairDStream.fromPairDStream(other.dstream, tag(), tag());
			return new PairRDS<K, Tuple2<V, W>>(s1.join(s2));
		default:
			throw new IllegalArgumentException();
		}
	}

	public <W> PairRDS<K, Tuple2<V, Optional<W>>> leftOuterJoin(PairRDS<K, W> other) {
		switch (type) {
		case RDD:
			JavaPairRDD<K, V> r1 = JavaPairRDD.fromRDD(union(rdds), tag(), tag());
			JavaPairRDD<K, W> r2 = JavaPairRDD.fromRDD(union(other.rdds), tag(), tag());
			return new PairRDS<K, Tuple2<V, Optional<W>>>(r1.leftOuterJoin(r2));
		case DSTREAM:
			JavaPairDStream<K, V> s1 = JavaPairDStream.fromPairDStream(dstream, tag(), tag());
			JavaPairDStream<K, W> s2 = JavaPairDStream.fromPairDStream(other.dstream, tag(), tag());
			return new PairRDS<K, Tuple2<V, Optional<W>>>(s1.leftOuterJoin(s2));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final <K2, V2> PairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		switch (type) {
		case RDD:
			Function<RDD<Tuple2<K, V>>, RDD<Tuple2<K2, V2>>> f = rdd -> JavaRDD.fromRDD(rdd, tag()).mapToPair(func).rdd();
			RDD<Tuple2<K2, V2>>[] r = each(rdds, f);
			return new PairRDS<K2, V2>(r);
		case DSTREAM:
			return new PairRDS<K2, V2>(JavaDStream.fromDStream(dstream, tag()).mapToPair(func));
		default:
			throw new IllegalArgumentException();
		}
	}
}
