package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

public class PairRDS<K, V> extends RDS<Tuple2<K, V>> {
	private static final long serialVersionUID = 2213547213519725138L;

	protected PairRDS() {}

	@Override
	protected PairRDS<K, V> init(DStream<Tuple2<K, V>> dstream) {
		super.init(dstream);
		return this;
	}

	@Override
	protected PairRDS<K, V> init(List<RDD<Tuple2<K, V>>> rdds) {
		super.init(rdds);
		return this;
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

	public Map<K, V> collectAsMap() {
		Map<K, V> r = new HashMap<>();
		eachRDD(rdd -> {
			for (Tuple2<K, V> t : rdd.collect())
				r.put(t._1, t._2);
		});
		return r;
	}

	public Set<K> collectKeys() {
		Set<K> r = new HashSet<>();
		eachRDD(rdd -> {
			for (Tuple2<K, V> t : rdd.collect())
				r.add(t._1);
		});
		return r;
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

	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func, int numPartitions) {
		switch (type) {
		case RDD:
			return new PairRDS<K, V>(JavaPairRDD.fromRDD(union(rdds), tag(), tag()).reduceByKey(func, numPartitions));
		case DSTREAM:
			return new PairRDS<K, V>(JavaPairDStream.fromPairDStream(dstream, tag(), tag()).reduceByKey(func, numPartitions));
		default:
			throw new IllegalArgumentException();
		}
	}

	public <W> PairRDS<K, Tuple2<V, W>> join(PairRDS<K, W> other) {
		switch (type) {
		case RDD:
			JavaPairRDD<K, V> r1 = JavaPairRDD.fromRDD(union(rdds), tag(), tag());
			switch (other.type) {
			case RDD:
				JavaPairRDD<K, W> r2 = JavaPairRDD.fromRDD(union(other.rdds), tag(), tag());
				return new PairRDS<K, Tuple2<V, W>>(r1.join(r2));
			case DSTREAM:
				JavaPairDStream<K, V> s1 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST, () -> r1);
				JavaPairDStream<K, W> s2 = JavaPairDStream.fromPairDStream(other.dstream, tag(), tag());
				return new PairRDS<K, Tuple2<V, W>>(s1.join(s2));
			}
		case DSTREAM:
			JavaPairDStream<K, V> s1 = JavaPairDStream.fromPairDStream(dstream, tag(), tag());
			switch (other.type) {
			case RDD:
				JavaPairDStream<K, W> s2 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST,
						() -> JavaPairRDD.fromRDD(union(other.rdds), tag(), tag()));
				return new PairRDS<K, Tuple2<V, W>>(s1.join(s2));
			case DSTREAM:
				return new PairRDS<K, Tuple2<V, W>>(s1.join(JavaPairDStream.fromPairDStream(other.dstream, tag(), tag())));
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	public <W> PairRDS<K, Tuple2<V, W>> join(PairRDS<K, W> other, int numPartitions) {
		switch (type) {
		case RDD:
			JavaPairRDD<K, V> r1 = JavaPairRDD.fromRDD(union(rdds), tag(), tag());
			switch (other.type) {
			case RDD:
				return new PairRDS<K, Tuple2<V, W>>(r1.join(JavaPairRDD.fromRDD(union(other.rdds), tag(), tag()), numPartitions));
			case DSTREAM:
				JavaPairDStream<K, V> s1 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST, () -> r1);
				JavaPairDStream<K, W> s2 = JavaPairDStream.fromPairDStream(other.dstream, tag(), tag());
				return new PairRDS<K, Tuple2<V, W>>(s1.join(s2, numPartitions));
			}
		case DSTREAM:
			JavaPairDStream<K, V> s1 = JavaPairDStream.fromPairDStream(dstream, tag(), tag());
			switch (other.type) {
			case RDD:
				JavaPairDStream<K, W> s2 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST,
						() -> JavaPairRDD.fromRDD(union(other.rdds), tag(), tag()));
				return new PairRDS<K, Tuple2<V, W>>(s1.join(s2, numPartitions));
			case DSTREAM:
				return new PairRDS<K, Tuple2<V, W>>(s1.join(JavaPairDStream.fromPairDStream(other.dstream, tag(), tag()), numPartitions));
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	public <W> PairRDS<K, Tuple2<V, Optional<W>>> leftOuterJoin(PairRDS<K, W> other) {
		switch (type) {
		case RDD:
			JavaPairRDD<K, V> r1 = JavaPairRDD.fromRDD(union(rdds), tag(), tag());
			switch (other.type) {
			case RDD:
				JavaPairRDD<K, W> r2 = JavaPairRDD.fromRDD(union(other.rdds), tag(), tag());
				return new PairRDS<K, Tuple2<V, Optional<W>>>(r1.leftOuterJoin(r2));
			case DSTREAM:
				JavaPairDStream<K, V> s1 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST, () -> r1);
				JavaPairDStream<K, W> s2 = JavaPairDStream.fromPairDStream(other.dstream, tag(), tag());
				return new PairRDS<K, Tuple2<V, Optional<W>>>(s1.leftOuterJoin(s2));
			}
		case DSTREAM:
			JavaPairDStream<K, V> s1 = JavaPairDStream.fromPairDStream(dstream, tag(), tag());
			switch (other.type) {
			case RDD:
				JavaPairDStream<K, W> s2 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST,
						() -> JavaPairRDD.fromRDD(union(other.rdds), tag(), tag()));
				return new PairRDS<K, Tuple2<V, Optional<W>>>(s1.leftOuterJoin(s2));
			case DSTREAM:
				return new PairRDS<K, Tuple2<V, Optional<W>>>(
						s1.leftOuterJoin(JavaPairDStream.fromPairDStream(other.dstream, tag(), tag())));
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	public <W> PairRDS<K, Tuple2<V, Optional<W>>> leftOuterJoin(PairRDS<K, W> other, int numPartitions) {
		switch (type) {
		case RDD:
			JavaPairRDD<K, V> r1 = JavaPairRDD.fromRDD(union(rdds), tag(), tag());
			JavaPairRDD<K, W> r2 = JavaPairRDD.fromRDD(union(other.rdds), tag(), tag());
			return new PairRDS<K, Tuple2<V, Optional<W>>>(r1.leftOuterJoin(r2, numPartitions));
		case DSTREAM:
			JavaPairDStream<K, V> s1 = JavaPairDStream.fromPairDStream(dstream, tag(), tag());
			switch (other.type) {
			case RDD:
				JavaPairDStream<K, W> s2 = RDDDStream.pstream(other.dstream.ssc(), Mechanism.CONST,
						() -> JavaPairRDD.fromRDD(union(other.rdds), tag(), tag()));
				return new PairRDS<K, Tuple2<V, Optional<W>>>(s1.leftOuterJoin(s2, numPartitions));
			case DSTREAM:
				return new PairRDS<K, Tuple2<V, Optional<W>>>(
						s1.leftOuterJoin(JavaPairDStream.fromPairDStream(other.dstream, tag(), tag()), numPartitions));
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	public final <K2, V2> PairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		switch (type) {
		case RDD:
			Function<RDD<Tuple2<K, V>>, RDD<Tuple2<K2, V2>>> f = rdd -> JavaRDD.fromRDD(rdd, tag()).mapToPair(func).rdd();
			List<RDD<Tuple2<K2, V2>>> r = each(rdds, f);
			return new PairRDS<K2, V2>().init(r);
		case DSTREAM:
			return new PairRDS<K2, V2>(JavaDStream.fromDStream(dstream, tag()).mapToPair(func));
		default:
			throw new IllegalArgumentException();
		}
	}

	public ClassTag<V> v() {
		return tag();
	}
}
