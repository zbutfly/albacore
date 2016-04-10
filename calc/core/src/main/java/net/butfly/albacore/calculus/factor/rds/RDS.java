package net.butfly.albacore.calculus.factor.rds;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.EmptyRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.lambda.Function;
import net.butfly.albacore.calculus.lambda.Function2;
import net.butfly.albacore.calculus.lambda.PairFunction;
import net.butfly.albacore.calculus.lambda.VoidFunction;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;
import scala.runtime.BoxedUnit;

public class RDS<T> implements Serializable {
	private static final long serialVersionUID = 5884484452918532597L;

	enum RDSType {
		RDD, DSTREAM
	}

	protected transient RDSType type;
	protected transient List<RDD<T>> rdds;
	protected transient DStream<T> dstream;

	protected RDS() {}

	protected RDS<T> init(List<RDD<T>> rdds) {
		type = RDSType.RDD;
		this.rdds = rdds;
		return this;
	}

	protected RDS<T> init(DStream<T> dstream) {
		type = RDSType.DSTREAM;
		this.dstream = dstream;
		return this;
	}

	@SafeVarargs
	public RDS(JavaRDDLike<T, ?>... rdd) {
		init(trans(Arrays.asList(rdd), r1 -> r1.rdd()));
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
			rdds = new ArrayList<>(Reflections.transform(rdds, r -> r.cache()));
			break;
		case DSTREAM:
			dstream = dstream.cache();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public RDS<T> persist() {
		switch (type) {
		case RDD:
			rdds = new ArrayList<>(Reflections.transform(rdds, r -> r.persist()));
			break;
		case DSTREAM:
			dstream = dstream.persist();
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
		eachRDD(rdd -> r.addAll(rdd.collect()));
		return r;
	}

	public RDS<T> eachRDD(VoidFunction<JavaRDD<T>> consumer) {
		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				consumer.call(JavaRDD.fromRDD(rdd, tag()));
			break;
		case DSTREAM:
			dstream.foreachRDD(rdd -> {
				consumer.call(JavaRDD.fromRDD(rdd, tag()));
				return BoxedUnit.UNIT;
			});
			break;
		}
		return this;
	}

	public RDS<T> each(VoidFunction<T> consumer) {

		switch (type) {
		case RDD:
			for (RDD<T> rdd : rdds)
				rdd.foreach(t -> {
					consumer.call(t);
					return BoxedUnit.UNIT;
				});
			break;
		case DSTREAM:
			dstream.foreachRDD(rdd -> {
				rdd.foreach(t -> {
					consumer.call(t);
					return BoxedUnit.UNIT;
				});
				return BoxedUnit.UNIT;
			});
			break;
		}
		return this;
	}

	public RDS<T> filter(Function<T, Boolean> func) {
		switch (type) {
		case RDD:
			rdds = trans(rdds, r -> JavaRDD.fromRDD(r, tag()).filter(t -> func.call(t)).rdd());
			break;
		case DSTREAM:
			dstream = JavaDStream.fromDStream(dstream, tag()).filter(t -> func.call(t)).dstream();
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	public <K2, V2> RDS<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		switch (type) {
		case RDD:
			return new RDS<Tuple2<K2, V2>>().init(trans(rdds, rdd -> JavaRDD.fromRDD(rdd, tag()).mapToPair(t -> func.call(t)).rdd()));
		case DSTREAM:
			return new RDS<Tuple2<K2, V2>>(JavaDStream.fromDStream(dstream, tag()).mapToPair(t -> func.call(t)));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final <T1> RDS<T1> map(Function<T, T1> func) {
		switch (type) {
		case RDD:
			return new RDS<T1>().init(trans(rdds, rdd -> rdd.map(t -> func.call(t), tag())));
		case DSTREAM:
			return new RDS<T1>().init(dstream.map(t -> func.call(t), tag()));
		default:
			throw new IllegalArgumentException();
		}
	}

	public final T reduce(Function2<T, T, T> func) {
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
			dstream.count().foreachRDD(rl -> {
				for (long l : (Long[]) rl.collect())
					r[0] += l;
				return BoxedUnit.UNIT;
			});
		default:
			throw new IllegalArgumentException();
		}
		return r[0];
	}

	@SuppressWarnings("unchecked")
	public static <T> ClassTag<T> tag() {
		return (ClassTag<T>) ManifestFactory.AnyRef();
	}

	protected final RDS<T> rddlike() {
		switch (type) {
		case RDD:
			if (rdds.size() <= 1) return this;
			RDD<T> r0 = rdds.get(0);
			for (int i = 1; i < rdds.size(); i++)
				r0 = r0.union(rdds.get(1));
			rdds = Arrays.asList(r0);
			break;
		case DSTREAM:
			this.type = RDSType.RDD;
			rdds = Arrays.asList(new EmptyRDD<T>(dstream.ssc().sc(), tag()));
			dstream.foreachRDD(rdd -> {
				rdds.set(0, rdds.get(0).union(rdd));
				return BoxedUnit.UNIT;
			});
			break;
		default:
			throw new IllegalArgumentException();
		}
		return this;
	}

	static <T, T1> List<T1> trans(List<T> r, Function<T, T1> transformer) {
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
		s.foreachRDD(v1 -> {
			if (l.isEmpty()) l.add(v1);
			else l.set(0, l.get(0).union(v1));
			return BoxedUnit.UNIT;
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
