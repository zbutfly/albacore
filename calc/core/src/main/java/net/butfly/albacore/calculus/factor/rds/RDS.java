package net.butfly.albacore.calculus.factor.rds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.lambda.Func2;
import net.butfly.albacore.calculus.lambda.PairFunc;
import net.butfly.albacore.calculus.lambda.VoidFunc;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;

public class RDS<T> implements Serializable {
	private static final long serialVersionUID = 5884484452918532597L;

	enum RDSType {
		RDD, DSTREAM
	}

	protected transient RDSType type;
	protected transient List<RDD<T>> rdds;
	protected transient DStream<T> dstream;

	protected RDS() {}

	protected RDS<T> init(Collection<RDD<T>> rdds) {
		type = RDSType.RDD;
		this.rdds = new ArrayList<>(rdds);
		return this;
	}

	protected RDS<T> init(DStream<T> dstream) {
		type = RDSType.DSTREAM;
		this.dstream = dstream;
		return this;
	}

	@SafeVarargs
	public RDS(JavaRDDLike<T, ?>... rdd) {
		List<RDD<T>> rs = trans(Arrays.asList(rdd), r -> r.rdd());
		init(rs);
	}

	public RDS(JavaDStreamLike<T, ?, ?> dstream) {
		init(dstream.dstream());
	}

	@SafeVarargs
	public RDS(JavaSparkContext sc, T... t) {
		this(sc.parallelize(Arrays.asList(t)));
	}

	public RDS<T> cache() {
		switch (type) {
		case RDD:
			rdds = Reflections.transform(rdds, RDD<T>::cache);
			break;
		case DSTREAM:
			dstream = dstream.cache();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public int partitions() {
		switch (type) {
		case RDD:
			int p = 0;
			for (RDD<T> rdd : rdds)
				p += rdd.partitions().length;
			return p;
		case DSTREAM:
			return -1;
		default:
			throw new IllegalArgumentException();
		}
	}

	public RDS<T> repartition(int numPartitions) {
		switch (type) {
		case RDD:
			rdds = Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()).repartition(numPartitions).rdd());
			break;
		case DSTREAM:
			dstream = dstream.repartition(numPartitions);
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public RDS<T> unpersist() {
		switch (type) {
		case RDD:
			Reflections.transform(rdds, v -> v.unpersist(true));
			break;
		case DSTREAM:
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public RDS<T> persist() {
		switch (type) {
		case RDD:
			rdds = Reflections.transform(rdds, RDD<T>::persist);
			break;
		case DSTREAM:
			dstream = dstream.persist();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public RDS<T> persist(StorageLevel level) {
		switch (type) {
		case RDD:
			rdds = Reflections.transform(rdds, v -> v.persist(level));
			break;
		case DSTREAM:
			dstream = dstream.persist(level);
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public final boolean isEmpty() {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				if (!rdd.isEmpty()) return false;
			return true;
		default:
			return false;
		}
	}

	public List<T> collect() {
		List<T> r = new ArrayList<>();
		each(r::add);
		return r;
	}

	public RDS<T> eachRDD(VoidFunc<JavaRDD<T>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				consumer.call(JavaRDD.fromRDD(rdd, tag()));
			break;
		case DSTREAM:
			dstream = JavaDStream.fromDStream(dstream, tag()).transform((Function<JavaRDD<T>, JavaRDD<T>>) rdd -> {
				consumer.call(rdd);
				return rdd;
			}).dstream();
			break;
		}
		return this;
	}

	public RDS<T> each(VoidFunc<T> consumer) {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				JavaRDD.fromRDD(rdd, tag()).foreach(consumer::call);;
			break;
		case DSTREAM:
			JavaDStream.fromDStream(dstream, tag()).transform((Function<JavaRDD<T>, JavaRDD<T>>) rdd -> {
				rdd.foreach(consumer::call);
				return rdd;
			});
			break;
		}
		return this;
	}

	public RDS<T> union(RDS<T> other) {
		switch (type) {
		case RDD:
			switch (other.type) {
			case RDD:
				rdds.addAll(other.rdds);
				break;
			case DSTREAM:
				JavaDStream.fromDStream(dstream, tag()).foreachRDD(r -> {
					rdds.add(r.rdd());
					return null;
				});
				break;
			default:
				throw new IllegalArgumentException();
			}
			break;
		case DSTREAM:
			switch (other.type) {
			case RDD:
				if (other.rdds.size() == 1) dstream = dstream.union(RDDDStream.stream(dstream.ssc(), Mechanism.CONST, () -> JavaRDD.fromRDD(
						other.rdds.get(0), tag())).dstream());
				break;
			case DSTREAM:
				dstream = dstream.union(other.dstream);
				break;
			default:
				throw new IllegalArgumentException();
			}
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	// XXX
	public RDS<T> filter(Func<T, Boolean> func) {
		switch (type) {
		case RDD:
			rdds = trans(rdds, r -> JavaRDD.fromRDD(r, tag()).filter(func::call).rdd());
			break;
		case DSTREAM:
			dstream = JavaDStream.fromDStream(dstream, tag()).filter(func::call).dstream();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public <K2, V2> RDS<Tuple2<K2, V2>> mapToPair(PairFunc<T, K2, V2> func) {
		switch (type) {
		case RDD:
			return new RDS<Tuple2<K2, V2>>().init(trans(rdds, (RDD<T> rdd) -> JavaRDD.fromRDD(rdd, tag()).mapToPair(func::call).rdd()));
		case DSTREAM:
			return new RDS<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, tag()).mapToPair(func::call));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final <T1> RDS<T1> map(Func<T, T1> func) {
		switch (type) {
		case RDD:
			return new RDS<T1>().init(trans(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()).map(func::call).rdd()));
		case DSTREAM:
			return new RDS<T1>().init(JavaDStream.fromDStream(dstream, tag()).map(func::call).dstream());
		default:
			throw new IllegalArgumentException();
		}
	}

	public <U> PairRDS<U, Iterable<T>> groupBy(Func<T, U> func) {
		return new PairRDS<U, Iterable<T>>(rdd().groupBy(func::call));
	}

	public <U> PairRDS<U, Iterable<T>> groupBy(Func<T, U> func, Comparator<T> sorting) {
		return new PairRDS<U, Iterable<T>>(rdd().groupBy(func::call));
	}

	public final T reduce(Func2<T, T, T> func) {
		scala.runtime.AbstractFunction2<T, T, T> f = new scala.runtime.AbstractFunction2<T, T, T>() {
			@Override
			public T apply(T v1, T v2) {
				return func.call(v1, v2);
			}
		};
		switch (type) {
		case RDD:
			return union(rdds).reduce(f);
		case DSTREAM:
			return union(dstream).reduce(f);
		default:
			throw new IllegalArgumentException();
		}
	}

	public final long count() {
		long[] r = new long[] { 0 };
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				r[0] += rdd.count();
			break;
		case DSTREAM:
			Function<JavaRDD<Long>, Void> foreachFunc = (Function<JavaRDD<Long>, Void>) rdd -> {
				r[0] += rdd.first();
				return null;
			};
			JavaDStream.fromDStream(dstream, tag()).count().foreachRDD(foreachFunc);
		default:
			throw new IllegalArgumentException();
		}
		return r[0];
	}

	@SuppressWarnings("unchecked")
	public static <T> ClassTag<T> tag() {
		return (ClassTag<T>) ManifestFactory.AnyRef();
	}

	public JavaRDD<T> rdd() {
		switch (type) {
		case RDD:
			if (rdds.size() == 0) return null;
			else {
				RDD<T> r = rdds.get(0);
				for (int i = 1; i < rdds.size(); i++)
					r = r.union(rdds.get(i));
				for (RDD<T> rr : rdds)
					rr.unpersist(false);
				return JavaRDD.fromRDD(r, tag());
			}
		case DSTREAM:
			List<RDD<T>> rs = new ArrayList<>();
			rs.add(new EmptyRDD<T>(dstream.ssc().sc(), tag()));
			JavaDStream.fromDStream(dstream, tag()).foreachRDD((Function<JavaRDD<T>, Void>) rdd -> {
				rs.set(0, rs.get(0).union(rdd.rdd()));
				return null;
			});
			return JavaRDD.fromRDD(rs.get(0), tag());
		default:
			throw new IllegalArgumentException();
		}
	}

	public Collection<JavaRDD<T>> rdds() {
		switch (type) {
		case RDD:
			return Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()));
		case DSTREAM:
			List<JavaRDD<T>> r = new ArrayList<>();
			eachRDD(r::add);
			return r;
		default:
			throw new IllegalArgumentException();
		}
	}

	public <S> RDS<T> sortBy(Func<T, S> comp) {
		JavaRDD<T> rdd = rdd();
		return new RDS<T>(rdd.sortBy(comp::call, true, rdd.partitions().size()));
	}

	static <T, T1> List<T1> trans(List<T> r, Func<T, T1> transformer) {
		if (r == null) return null;
		List<T1> r1 = new ArrayList<>(r.size());
		for (T rr : r)
			try {
				r1.add(transformer.call(rr));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		return r1;
	}

	static <T> RDD<T> union(List<RDD<T>> r) {
		if (r == null || r.size() == 0) return null;
		List<RDD<T>> rr = new ArrayList<>(r);
		for (int i = 1; i > rr.size(); i++)
			rr.set(0, rr.get(0).union(rr.get(i)));
		return rr.get(0);
	}

	@SuppressWarnings({ "unchecked" })
	static <T, R extends JavaRDDLike<T, R>, S extends JavaDStreamLike<T, S, R>> R union(S s) {
		List<R> l = new ArrayList<>();
		s.foreachRDD((Function<R, Void>) r -> {
			if (l.isEmpty()) l.add(r);
			else l.set(0, (R) JavaRDD.fromRDD(l.get(0).rdd().union(r.rdd()), l.get(0).classTag()));
			return null;
		});
		return l.get(0);
	}

	@SuppressWarnings("unchecked")
	static <T> RDD<T> union(RDD<T>... r) {
		if (r == null || r.length == 0) return null;
		RDD<T>[] rr = r;
		for (int i = 1; i > rr.length; i++)
			rr[0] = rr[0].union(r[i]);
		return rr[0];
	}

	static <T> RDD<T> union(DStream<T> s) {
		List<RDD<T>> l = new ArrayList<>();
		JavaDStream.fromDStream(s, tag()).foreachRDD((Function<JavaRDD<T>, Void>) rdd -> {
			if (l.isEmpty()) l.add(rdd.rdd());
			else l.set(0, l.get(0).union(rdd.rdd()));
			return null;
		});
		return l.get(0);
	}

	static <K, V> JavaPairRDD<K, V>[] pair(JavaRDD<Tuple2<K, V>>[] rdds) {
		@SuppressWarnings("unchecked")
		JavaPairRDD<K, V>[] prdds = new JavaPairRDD[rdds.length];
		for (int i = 0; i < rdds.length; i++)
			prdds[i] = rdds[i].mapToPair(t -> t);
		return prdds;
	}

	static <K, V> JavaRDD<Tuple2<K, V>>[] unpair(JavaPairRDD<K, V>[] prdds) {
		@SuppressWarnings("unchecked")
		JavaRDD<Tuple2<K, V>>[] rdds = new JavaRDD[prdds.length];
		for (int i = 0; i < prdds.length; i++)
			rdds[i] = prdds[i].map(t -> t);
		return rdds;
	}
}
