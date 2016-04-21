package net.butfly.albacore.calculus.factor.rds;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;

import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.lambda.Func2;
import net.butfly.albacore.calculus.lambda.PairFunc;
import net.butfly.albacore.calculus.lambda.VoidFunc;
import net.butfly.albacore.calculus.lambda.VoidFunc2;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class PairRDS<K, V> extends RDS<Tuple2<K, V>> {
	private static final long serialVersionUID = 2213547213519725138L;

	protected PairRDS() {}

	@Override
	protected PairRDS<K, V> init(DStream<Tuple2<K, V>> dstream) {
		super.init(dstream);
		return this;
	}

	@Override
	protected PairRDS<K, V> init(Collection<RDD<Tuple2<K, V>>> rdds) {
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

	public PairRDS(Collection<PairRDS<K, V>> l) {
		this();
		List<RDD<Tuple2<K, V>>> s = new ArrayList<>();
		for (Collection<RDD<Tuple2<K, V>>> rs : Reflections.transform(Reflections.transform(l, PairRDS<K, V>::rdds),
				(final Collection<JavaRDD<Tuple2<K, V>>> rs) -> Reflections.transform(rs, JavaRDD<Tuple2<K, V>>::rdd)))
			s.addAll(rs);
		init(s);
	}

	public Map<K, V> collectAsMap() {
		Map<K, V> r = new HashMap<>();
		each((VoidFunc<Tuple2<K, V>>) t -> r.put(t._1, t._2));
		return r;
	}

	public Set<K> collectKeys() {
		Set<K> r = new HashSet<>();
		this.each((VoidFunc<Tuple2<K, V>>) t -> r.add(t._1));
		return r;
	}

	public void eachPairRDD(VoidFunc<JavaPairRDD<K, V>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<Tuple2<K, V>> rdd : rdds)
				consumer.call(JavaPairRDD.fromRDD(rdd, tag(), tag()));
			break;
		case DSTREAM:
			JavaPairDStream.fromPairDStream(dstream, tag(), tag()).foreachRDD((Function<JavaPairRDD<K, V>, Void>) rdd -> {
				consumer.call(rdd);
				return null;
			});
			break;
		}
	}

	public PairRDS<K, V> each(VoidFunc2<K, V> consumer) {
		switch (type) {
		case RDD:
			for (RDD<Tuple2<K, V>> rdd : rdds)
				JavaPairRDD.fromRDD(rdd, tag(), tag()).foreach(t -> consumer.call(t._1, t._2));
			break;
		case DSTREAM:
			JavaPairDStream.fromPairDStream(dstream, tag(), tag()).foreachRDD((Function<JavaPairRDD<K, V>, Void>) rdd -> {
				rdd.foreach(t -> consumer.call(t._1, t._2));
				return null;
			});
			break;
		}
		return this;
	}

	public <U> PairRDS<K, Iterable<V>> groupByKey() {
		return new PairRDS<K, Iterable<V>>(pairRDD().groupByKey());
	}

	public <U> PairRDS<K, Iterable<V>> groupByKeyWithSort(Comparator<V> comp) {
		return new PairRDS<K, Iterable<V>>(pairRDD().groupByKey());
	}

	public PairRDS<K, V> reduceByKey(Func2<V, V, V> func) {
		switch (type) {
		case RDD:
			return new PairRDS<K, V>(JavaPairRDD.fromRDD(union(rdds), tag(), tag()).reduceByKey(func::call));
		case DSTREAM:
			return new PairRDS<K, V>(JavaPairDStream.fromPairDStream(dstream, tag(), tag()).reduceByKey(func::call));
		default:
			throw new IllegalArgumentException();
		}
	}

	public PairRDS<K, V> reduceByKey(Func2<V, V, V> func, int numPartitions) {
		switch (type) {
		case RDD:
			return new PairRDS<K, V>(JavaPairRDD.fromRDD(union(rdds), tag(), tag()).reduceByKey(func::call, numPartitions));
		case DSTREAM:
			return new PairRDS<K, V>(
					JavaPairDStream.fromPairDStream(dstream, tag(), tag()).reduceByKey((Function2<V, V, V>) func::call, numPartitions));
		default:
			throw new IllegalArgumentException();
		}
	}

	public PairRDS<K, V> union(PairRDS<K, V> other) {
		super.union(other);
		return this;
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

	public Collection<JavaPairRDD<K, V>> pairRDDs() {
		switch (type) {
		case RDD:
			return Reflections.transform(rdds, rdd -> JavaPairRDD.fromRDD(rdd, tag(), tag()));
		case DSTREAM:
			List<JavaPairRDD<K, V>> r = new ArrayList<>();
			eachPairRDD(r::add);
			return r;
		default:
			throw new IllegalArgumentException();
		}
	}

	public JavaPairRDD<K, V> pairRDD() {
		switch (type) {
		case RDD:
			if (rdds.size() == 0) return null;
			else {
				RDD<Tuple2<K, V>> r = rdds.get(0);
				for (int i = 1; i < rdds.size(); i++)
					r = r.union(rdds.get(i));
				return JavaPairRDD.fromRDD(r, tag(), tag());
			}
		case DSTREAM:
			List<RDD<Tuple2<K, V>>> rs = new ArrayList<>();
			rs.add(new EmptyRDD<Tuple2<K, V>>(dstream.ssc().sc(), tag()));
			JavaPairDStream.fromPairDStream(dstream, tag(), tag()).foreachRDD((Function<JavaPairRDD<K, V>, Void>) rdd -> {
				rs.set(0, rs.get(0).union(rdd.rdd()));
				return null;
			});
			return JavaPairRDD.fromRDD(rs.get(0), tag(), tag());
		default:
			throw new IllegalArgumentException();
		}
	}

	public <S> PairRDS<K, V> sortBy(Func<Tuple2<K, V>, S> comp) {
		JavaRDD<Tuple2<K, V>> rdd = rdd();
		return new PairRDS<K, V>(rdd().sortBy(comp::call, true, rdd.partitions().size()));
	}

	public PairRDS<K, V> sortByKey(boolean asc) {
		return new PairRDS<>(pairRDD().sortByKey(asc));
	}

	public Tuple2<K, V> first() {
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
	public PairRDS<K, V> filter(Func<Tuple2<K, V>, Boolean> func) {
		switch (type) {
		case RDD:
			rdds = trans(rdds, r -> JavaPairRDD.fromRDD(r, tag(), tag()).filter(func::call).rdd());
			break;
		case DSTREAM:
			dstream = JavaPairDStream.fromPairDStream(dstream, tag(), tag()).filter(func::call).dstream();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public final <K2, V2> PairRDS<K2, V2> mapPair(PairFunc<Tuple2<K, V>, K2, V2> func) {
		switch (type) {
		case RDD:
			return new PairRDS<K2, V2>().init(RDS.trans(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()).mapToPair(func::call).rdd()));
		case DSTREAM:
			return new PairRDS<K2, V2>(JavaDStream.fromDStream(dstream, tag()).mapToPair(func::call));
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

	public <RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, DataDetail<V> dd) {
		eachPairRDD((final JavaPairRDD<K, V> rdd) -> rdd.mapToPair((final Tuple2<K, V> t) -> (Tuple2<WK, WV>) ds.writing(t._1, t._2))
				.filter(t -> t != null && t._1 != null && t._2 != null)
				.saveAsNewAPIHadoopFile("", ds.keyClass, ds.valueClass, ds.outputFormatClass, dd.outputConfiguration(ds)));
	}
}
