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
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;

import net.butfly.albacore.calculus.lambda.Function;
import net.butfly.albacore.calculus.lambda.Function2;
import net.butfly.albacore.calculus.lambda.PairFunction;
import net.butfly.albacore.calculus.lambda.VoidFunction;
import net.butfly.albacore.calculus.lambda.VoidFunction2;
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
		this.eachRDD(rdd -> {
			for (Tuple2<K, V> t : rdd.collect())
				if (t._1 != null) r.add(t._1);
		});
		return r;
	}

	public void eachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<Tuple2<K, V>> rdd : rdds)
				consumer.call(JavaPairRDD.fromRDD(rdd, tag(), tag()));
			break;
		case DSTREAM:
			dstream.foreachRDD(rdd -> {
				consumer.call(JavaPairRDD.fromRDD(rdd, tag(), tag()));
				return scala.runtime.BoxedUnit.UNIT;
			});
			break;
		}
	}

	public PairRDS<K, V> each(VoidFunction2<K, V> consumer) {
		switch (type) {
		case RDD:
			for (RDD<Tuple2<K, V>> rdd : rdds)
				JavaPairRDD.fromRDD(rdd, tag(), tag()).foreach(t -> consumer.call(t._1, t._2));
			break;
		case DSTREAM:
			dstream.foreachRDD(rdd -> {
				JavaPairRDD.fromRDD(rdd, tag(), tag()).foreach(t -> consumer.call(t._1, t._2));
				return BoxedUnit.UNIT;
			});
			break;
		}
		return this;
	}

	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		switch (type) {
		case RDD:
			return new PairRDS<K, V>(JavaPairRDD.fromRDD(union(rdds), tag(), tag()).reduceByKey((v1, v2) -> func.call(v1, v2)));
		case DSTREAM:
			return new PairRDS<K, V>(JavaPairDStream.fromPairDStream(dstream, tag(), tag()).reduceByKey((v1, v2) -> func.call(v1, v2)));
		default:
			throw new IllegalArgumentException();
		}
	}

	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func, int numPartitions) {
		switch (type) {
		case RDD:
			return new PairRDS<K, V>(
					JavaPairRDD.fromRDD(union(rdds), tag(), tag()).reduceByKey((v1, v2) -> func.call(v1, v2), numPartitions));
		case DSTREAM:
			return new PairRDS<K, V>(
					JavaPairDStream.fromPairDStream(dstream, tag(), tag()).reduceByKey((v1, v2) -> func.call(v1, v2), numPartitions));
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

	public PairRDS<K, V> sortByKey(boolean asc) {
		rddlike();
		if (rdds.size() > 0) rdds.set(0, JavaPairRDD.fromRDD(rdds.get(0), tag(), tag()).sortByKey(asc).rdd());
		return this;
	}

	public Tuple2<K, V> first() {
		rddlike();
		return rdds.size() > 0 ? rdds.get(0).first() : null;
	}

	public K maxKey() {
		@SuppressWarnings("unchecked")
		Ordering<K> c = (Ordering<K>) Ordering.natural();
		this.cache();
		return this.reduce((t1, t2) -> (c.compare(t1._1, t2._1) > 0 ? t1 : t2))._1;
	}

	public K minKey() {
		@SuppressWarnings("unchecked")
		Ordering<K> c = (Ordering<K>) Ordering.natural();
		return this.reduce((t1, t2) -> (c.compare(t1._1, t2._1) < 0 ? t1 : t2))._1;
	}

	@Override
	public PairRDS<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		switch (type) {
		case RDD:
			rdds = trans(rdds, r -> JavaPairRDD.fromRDD(r, tag(), tag()).filter(t -> func.call(t)).rdd());
			break;
		case DSTREAM:
			dstream = JavaPairDStream.fromPairDStream(dstream, tag(), tag()).filter(t -> func.call(t)).dstream();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public final <K2, V2> PairRDS<K2, V2> mapPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		switch (type) {
		case RDD:
			return new PairRDS<K2, V2>().init(RDS.trans(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()).mapToPair(t -> func.call(t)).rdd()));
		case DSTREAM:
			return new PairRDS<K2, V2>(JavaDStream.fromDStream(dstream, tag()).mapToPair(t -> func.call(t)));
		default:
			throw new IllegalArgumentException();
		}
	}

	public ClassTag<V> v() {
		return tag();
	}

	public PairRDS<K, V> cache() {
		super.cache();
		return this;
	}

	public PairRDS<K, V> persist() {
		super.persist();
		return this;
	}
}
