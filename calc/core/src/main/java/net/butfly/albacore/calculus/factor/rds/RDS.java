package net.butfly.albacore.calculus.factor.rds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.apache.spark.streaming.dstream.DStream;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;
import scala.runtime.BoxedUnit;

public class RDS<T> implements Serializable {
	private static final long serialVersionUID = 5884484452918532597L;

	enum RDSType {
		RDD, DSTREAM
	}

	protected RDSType type;
	protected RDD<T>[] rdds;
	protected DStream<T> dstream;

	protected RDS() {}

	@SafeVarargs
	protected RDS(RDD<T>... rdds) {
		type = RDSType.RDD;
		this.rdds = rdds;
	}

	protected RDS(DStream<T> dstream) {
		type = RDSType.DSTREAM;
		this.dstream = dstream;
	}

	@SafeVarargs
	public RDS(JavaRDDLike<T, ?>... rdd) {
		this(each(rdd, r -> r.rdd()));
	}

	public RDS(JavaDStreamLike<T, ?, ?> dstream) {
		this(dstream.dstream());
	}

	@SafeVarargs
	public RDS(JavaSparkContext sc, T... t) {
		this(sc.parallelize(Arrays.asList(t)));
	}

	public RDS<T> folk() {
		switch (type) {
		case RDD:
			RDS<T> n = new RDS<T>(each(rdds, v1 -> v1.cache()));
			return n;
		case DSTREAM:
			return new RDS<T>(dstream.cache());
		default:
			throw new IllegalArgumentException();
		}
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

	public RDS<T> eachRDD(VoidFunction<JavaRDD<T>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				try {
					consumer.call(JavaRDD.fromRDD(rdd, tag()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			break;
		case DSTREAM:
			dstream.foreachRDD(wrap(rdd -> {
				consumer.call(JavaRDD.fromRDD(rdd, tag()));
				return BoxedUnit.UNIT;
			}));
			break;
		}
		return this;
	}

	public <K2, V2> RDS<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		switch (type) {
		case RDD:
			Function<RDD<T>, RDD<Tuple2<K2, V2>>> f = rdd -> JavaRDD.fromRDD(rdd, tag()).mapToPair(func).rdd();
			RDD<Tuple2<K2, V2>>[] r = each(rdds, f);
			return new RDS<Tuple2<K2, V2>>(r);
		case DSTREAM:
			return new RDS<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, tag()).mapToPair(func));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final <T1> RDS<T1> map(Function<T, T1> func) {
		switch (type) {
		case RDD:
			return new RDS<T1>(each(rdds, rdd -> rdd.map(wrap(func), tag())));
		case DSTREAM:
			return new RDS<T1>(dstream.map(wrap(func), tag()));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final T reduce(Function2<T, T, T> func) {
		scala.runtime.AbstractFunction2<T, T, T> f = new scala.runtime.AbstractFunction2<T, T, T>() {
			@Override
			public T apply(T v1, T v2) {
				try {
					return func.call(v1, v2);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
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
			dstream.count().foreachRDD(wrap(rl -> {
				for (long l : (Long[]) rl.collect())
					r[0] += l;
				return BoxedUnit.UNIT;
			}));
		default:
			throw new IllegalArgumentException();
		}
		return r[0];
	}

	@SuppressWarnings("unchecked")
	static <T, T1> T1[] each(T[] r, Function<T, T1> transformer) {
		if (r == null) return null;
		List<T1> r1 = new ArrayList<>(r.length);
		for (T rr : r)
			try {
				r1.add(transformer.call(rr));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		return (T1[]) r1.toArray();
	}

	@SuppressWarnings("unchecked")
	static <T, R extends JavaRDDLike<T, R>> R union(R... r) {
		if (r == null || r.length == 0) return null;
		R[] rr = r;
		for (int i = 1; i > rr.length; i++)
			rr[0] = (R) JavaRDD.fromRDD(rr[0].rdd().union(r[i].rdd()), rr[0].classTag());
		return rr[0];
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	static <T, R extends JavaRDDLike<T, R>, S extends JavaDStreamLike<T, S, R>> R union(S s) {
		List<R> l = new ArrayList<>();
		s.foreachRDD(v1 -> {
			if (l.isEmpty()) l.add(v1);
			else l.set(0, (R) JavaRDD.fromRDD(l.get(0).rdd().union(v1.rdd()), l.get(0).classTag()));
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
		s.foreachRDD(wrap(v1 -> {
			if (l.isEmpty()) l.add(v1);
			else l.set(0, l.get(0).union(v1));
			return BoxedUnit.UNIT;
		}));
		return l.get(0);
	}

	static <T, R> scala.Function1<T, R> wrap(Function<T, R> f) {
		return new scala.runtime.AbstractFunction1<T, R>() {
			@Override
			public R apply(T v1) {
				try {
					return f.call(v1);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	@SuppressWarnings("unchecked")
	static <T> ClassTag<T> tag() {
		return (ClassTag<T>) ManifestFactory.AnyRef();
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
