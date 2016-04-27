package net.butfly.albacore.calculus.factor.rds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
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

	protected RDS(DStream<T> dstream) {
		type = RDSType.DSTREAM;
		this.dstream = dstream;
	}

	public RDS(JavaDStreamLike<T, ?, ?> dstream) {
		this(dstream.dstream());
	}

	@SafeVarargs
	protected RDS(RDD<T>... rdds) {
		this(Arrays.asList(rdds));
	}

	protected RDS(Collection<RDD<T>> rdds) {
		type = RDSType.RDD;
		this.rdds = new ArrayList<>(rdds);
	}

	@SafeVarargs
	public RDS(JavaRDDLike<T, ?>... rdd) {
		this(Reflections.transform(Arrays.asList(rdd), r -> r.rdd()));
	}

	@SafeVarargs
	public RDS(JavaSparkContext sc, T... t) {
		this(sc.parallelize(Arrays.asList(t)));
	}

	public int partitions() {
		switch (type) {
		case RDD:
			int p = 0;
			for (RDD<T> rdd : rdds)
				p += rdd.getNumPartitions();
			return p;
		case DSTREAM:
			return -1;
		default:
			throw new IllegalArgumentException();
		}
	}

	public RDS<T> repartition(float ratio) {
		switch (type) {
		case RDD:
			return new RDS<T>(Reflections.transform(rdds,
					rdd -> JavaRDD.fromRDD(rdd, tag()).repartition((int) Math.ceil(rdd.getNumPartitions() * ratio)).rdd()));
		case DSTREAM:
			return new RDS<T>(JavaDStream.fromDStream(dstream, tag()).transform(
					(Function<JavaRDD<T>, JavaRDD<T>>) rdd -> rdd.repartition((int) Math.ceil(rdd.partitions().size() * ratio))));
		default:
			throw new IllegalArgumentException();
		}
	}

	public RDS<T> unpersist() {
		switch (type) {
		case RDD:
			return new RDS<T>(Reflections.transform(rdds, v -> v.unpersist(true)));
		case DSTREAM:
			return this;
		default:
			throw new IllegalArgumentException();
		}
	}

	public RDS<T> persist() {
		switch (type) {
		case RDD:
			return new RDS<T>(rdds = Reflections.transform(rdds, RDD<T>::persist));
		case DSTREAM:
			return new RDS<T>(dstream.persist());
		default:
			throw new IllegalArgumentException();
		}
	}

	public RDS<T> persist(StorageLevel level) {
		switch (type) {
		case RDD:
			return new RDS<T>(Reflections.transform(rdds, v -> v.persist(level)));
		case DSTREAM:
			return new RDS<T>(dstream.persist(level));
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

	public List<T> collect() {
		List<T> r = new ArrayList<>();
		each(r::add);
		return r;
	}

	public void eachRDD(VoidFunc<JavaRDD<T>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				consumer.call(JavaRDD.fromRDD(rdd, tag()));
			break;
		case DSTREAM:
			JavaDStream.fromDStream(dstream, tag()).foreachRDD(consumer::call);
			break;
		}
	}

	public void each(VoidFunc<T> consumer) {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				JavaRDD.fromRDD(rdd, tag()).foreach(consumer::call);;
			break;
		case DSTREAM:
			JavaDStream.fromDStream(dstream, tag()).foreachRDD(rdd -> rdd.foreach(consumer::call));
			break;
		}
	}

	public RDS<T> union(RDS<T> other) {
		switch (type) {
		case RDD:
			switch (other.type) {
			case RDD:
				JavaRDD<T> r1 = rdd();
				JavaRDD<T> r2 = other.rdd();
				if (r1 == null) return new RDS<>(r1);
				if (null == r2) return new RDS<>(r2);
				return new RDS<>(r1.union(r2));
			case DSTREAM:
				return new RDS<>(JavaDStream.fromDStream(dstream, tag()).transform(r -> {
					rdds.add(r.rdd());
					return null;
				}));
			default:
				throw new IllegalArgumentException();
			}
		case DSTREAM:
			switch (other.type) {
			case RDD:
				JavaDStream<T> th = RDDDStream.stream(dstream.ssc(), Mechanism.CONST, () -> JavaRDD.fromRDD(other.rdd().rdd(), tag()));
				return new RDS<T>(dstream.union(th.dstream()));
			case DSTREAM:
				return new RDS<T>(dstream.union(other.dstream));
			default:
				throw new IllegalArgumentException();
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	// XXX
	public RDS<T> filter(Func<T, Boolean> func) {
		switch (type) {
		case RDD:
			return new RDS<T>(Reflections.transform(rdds, r -> JavaRDD.fromRDD(r, tag()).filter(func::call).rdd()));
		case DSTREAM:
			return new RDS<T>(JavaDStream.fromDStream(dstream, tag()).filter(func::call).dstream());
		default:
			throw new IllegalArgumentException();
		}
	}

	public <K2, V2> RDS<Tuple2<K2, V2>> mapToPair(PairFunc<T, K2, V2> func) {
		switch (type) {
		case RDD:
			return new RDS<Tuple2<K2, V2>>(
					Reflections.transform(rdds, (RDD<T> rdd) -> JavaRDD.fromRDD(rdd, tag()).mapToPair(func::call).rdd()));
		case DSTREAM:
			return new RDS<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, tag()).mapToPair(func::call));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final <T1> RDS<T1> map(Func<T, T1> func) {
		switch (type) {
		case RDD:
			return new RDS<T1>(Reflections.transform(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()).map(func::call).rdd()));
		case DSTREAM:
			return new RDS<T1>(JavaDStream.fromDStream(dstream, tag()).map(func::call).dstream());
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
			JavaDStream.fromDStream(dstream, tag()).count().foreachRDD((VoidFunction<JavaRDD<Long>>) rdd -> r[0] += rdd.first());
		default:
			throw new IllegalArgumentException();
		}
		return r[0];
	}

	public JavaRDD<T> rdd() {
		switch (type) {
		case RDD:
			if (rdds.size() == 0) return null;
			else {
				RDD<T> r = rdds.get(0);
				for (int i = 1; i < rdds.size(); i++)
					r = r.union(rdds.get(i));
				unpersist();
				return JavaRDD.fromRDD(r, tag());
			}
		case DSTREAM:
			List<RDD<T>> rs = new ArrayList<>();
			rs.add(new EmptyRDD<T>(dstream.ssc().sc(), tag()));
			JavaDStream.fromDStream(dstream, tag()).foreachRDD((VoidFunction<JavaRDD<T>>) rdd -> rs.set(0, rs.get(0).union(rdd.rdd())));
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

	public static <T> RDD<T> union(Collection<RDD<T>> r) {
		if (r == null || r.size() == 0) return null;
		Iterator<RDD<T>> it = r.iterator();
		if (r.size() == 1) return r.iterator().next();
		RDD<T> r0 = r.iterator().next();
		while (it.hasNext())
			r0 = r0.union(it.next());
		return r0;
	}

	@SuppressWarnings({ "unchecked" })
	public static <T, R extends JavaRDDLike<T, R>, S extends JavaDStreamLike<T, S, R>> R union(S s) {
		List<R> l = new ArrayList<>();
		s.foreachRDD(r -> {
			if (l.isEmpty()) l.add(r);
			else l.set(0, (R) JavaRDD.fromRDD(l.get(0).rdd().union(r.rdd()), l.get(0).classTag()));
		});
		return l.get(0);
	}

	@SuppressWarnings({ "unchecked" })
	public static <T, S extends JavaRDDLike<T, S>> S union(List<S> s) {
		if (s.isEmpty()) return null;
		if (s.size() == 1) return s.get(0);
		RDD<T> s0 = s.get(0).rdd();
		for (int i = 1; i < s.size(); i++)
			s0 = s0.union(s.get(i).rdd());
		return (S) JavaRDD.fromRDD(s0, tag());
	}

	@SuppressWarnings("unchecked")
	public static <T> RDD<T> union(RDD<T>... r) {
		if (r == null || r.length == 0) return null;
		RDD<T>[] rr = r;
		for (int i = 1; i > rr.length; i++)
			rr[0] = rr[0].union(r[i]);
		return rr[0];
	}

	public static <T> RDD<T> union(DStream<T> s) {
		List<RDD<T>> l = new ArrayList<>();
		JavaDStream.fromDStream(s, tag()).foreachRDD(rdd -> {
			if (l.isEmpty()) l.add(rdd.rdd());
			else l.set(0, l.get(0).union(rdd.rdd()));
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

	@SuppressWarnings("unchecked")
	public static <T> ClassTag<T> tag() {
		return (ClassTag<T>) ManifestFactory.AnyRef();
	}
}
