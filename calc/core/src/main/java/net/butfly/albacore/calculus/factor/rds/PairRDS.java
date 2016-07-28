package net.butfly.albacore.calculus.factor.rds;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.google.common.base.Optional;

import net.butfly.albacore.calculus.factor.rds.internal.EncoderBuilder;
import net.butfly.albacore.calculus.factor.rds.internal.PairWrapped;
import net.butfly.albacore.calculus.factor.rds.internal.Wrapped;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDStream;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDataFrame;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedDataset;
import net.butfly.albacore.calculus.factor.rds.internal.WrappedRDD;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import scala.Tuple2;

/**
 * Single including any implementation of spark data.
 * 
 * @author zx
 *
 * @param <K>
 * @param <V>
 */
@SuppressWarnings({ "deprecation", "unchecked" })
public final class PairRDS<K, V> extends RDS<Tuple2<K, V>> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 7112147100603052906L;

	protected PairRDS() {}

	public PairRDS(Wrapped<Tuple2<K, V>> wrapped) {
		super(wrapped);
	}

	@SafeVarargs
	public PairRDS(JavaSparkContext sc, Tuple2<K, V>... t) {
		super(sc, t);
	}

	@Override
	public PairRDS<K, V> unpersist() {
		return new PairRDS<>(wrapped().unpersist());
	}

	@Override
	public PairRDS<K, V> persist(StorageLevel level) {
		return new PairRDS<>(wrapped().persist(level));
	}

	@Override
	public PairRDS<K, V> repartition(float ratio) {
		return new PairRDS<>(wrapped().repartition(ratio));
	}

	@Override
	public PairRDS<K, V> sortByKey(boolean asc) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(w.sortByKey(asc));
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) return this; // stream can not be sorted
		return new PairRDS<>(new WrappedRDD<>(pairRDD().sortByKey(asc)));
	}

	@Override
	public <S> PairRDS<K, V> sortBy(Function2<K, V, S> comp, Class<?>... cls) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().sortBy(comp, cls));
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) return this; // stream can not be sorted
		JavaRDD<Tuple2<K, V>> j = jrdd();
		return new PairRDS<>(new WrappedRDD<>(j.sortBy(t -> comp.call(t._1, t._2), true, j.getNumPartitions())));
	}

	public PairRDS<K, V> union(PairWrapped<K, V> other) {
		return union(other.wrapped());
	}

	@Override
	public PairRDS<K, V> union(Wrapped<Tuple2<K, V>> other) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(wrapped().union(other));
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			JavaPairDStream<K, V> s2 = new PairRDS<>(other).pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.union(s2).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().union(JavaPairRDD.fromJavaRDD(other.jrdd()))));
	}

	@Override
	public PairRDS<K, V> filter(Function2<K, V, Boolean> func) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().filter(func));
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) return new PairRDS<>(new WrappedDStream<>(pairStream(ssc).filter(t -> func.call(t._1, t._2)).dstream()));
		return new PairRDS<>(new WrappedRDD<>(pairRDD().filter(t -> func.call(t._1, t._2))));
	}

	@Override
	public PairRDS<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		return new PairRDS<>(wrapped().filter(func));
	}

	@Override
	public final <K2, V2> PairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func, Class<?>... vClass2) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(wrapped().mapToPair(func, vClass2));
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) return new PairRDS<>(new WrappedDStream<>(pairStream(ssc).mapToPair(func).dstream()));
		return new PairRDS<>(new WrappedRDD<>(pairRDD().mapToPair(func)));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().join(other.wrapped(), vClass2).wrapped());
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			JavaPairDStream<K, V2> s2 = new PairRDS<>(other).pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.join(s2).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().join(JavaPairRDD.fromJavaRDD(other.jrdd()))));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions, Class<?>... vClass2) {
		int pnum = (int) Math.ceil(Math.max(getNumPartitions(), other.getNumPartitions()) * ratioPartitions);
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().join(other.wrapped(), pnum, vClass2).wrapped());
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			JavaPairDStream<K, V2> s2 = new PairRDS<>(other).pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.join(s2, pnum).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().join(JavaPairRDD.fromJavaRDD(other.jrdd()), pnum)));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().leftOuterJoin(other.wrapped(), vClass2).wrapped());
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			JavaPairDStream<K, V2> s2 = new PairRDS<>(other).pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.leftOuterJoin(s2).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().leftOuterJoin(JavaPairRDD.fromJavaRDD(other.jrdd()))));
	}

	@Override
	public <V2> PairRDS<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, float ratioPartitions,
			Class<?>... vClass2) {
		int pnum = (int) Math.ceil(Math.max(getNumPartitions(), other.getNumPartitions()) * ratioPartitions);
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().leftOuterJoin(other.wrapped(), pnum, vClass2).wrapped());
		StreamingContext ssc = Wrapped.streaming(this, other);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			JavaPairDStream<K, V2> s2 = new PairRDS<>(other).pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.leftOuterJoin(s2, pnum).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().leftOuterJoin(JavaPairRDD.fromJavaRDD(other.jrdd()), pnum)));
	}

	@Override
	@Deprecated
	public <U> PairRDS<K, Iterable<V>> groupByKey() {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().groupByKey().wrapped());
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) return new PairRDS<>(new WrappedDStream<>(pairStream(ssc).groupByKey().dstream()));
		return new PairRDS<>(new WrappedRDD<>(pairRDD().groupByKey()));
	}

	@Override
	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().reduceByKey(func).wrapped());
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.reduceByKey(func).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().reduceByKey(func)));
	}

	@Override
	public PairRDS<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions) {
		int pnum = (int) Math.ceil(getNumPartitions() * ratioPartitions);
		PairWrapped<K, V> w = pairWrapped();
		if (null != w) return new PairRDS<>(pairWrapped().reduceByKey(func, pnum).wrapped());
		StreamingContext ssc = Wrapped.streaming(this);
		if (null != ssc) {
			JavaPairDStream<K, V> s1 = pairStream(ssc);
			return new PairRDS<>(new WrappedDStream<>(s1.reduceByKey(func, pnum).dstream()));
		}
		return new PairRDS<>(new WrappedRDD<>(pairRDD().reduceByKey(func, pnum)));
	}

	@Override
	public WrappedDataset<K, V> toDS(Class<?>... vClass) {
		if (wrapped instanceof WrappedDataset) return (WrappedDataset<K, V>) wrapped;
		JavaRDD<Tuple2<K, V>> rdd = wrapped().jrdd();
		SQLContext ssc = new SQLContext(rdd.context());
		return new WrappedDataset<K, V>(ssc, rdd.map(t -> t._2), EncoderBuilder.with(vClass));
	}

	@Override
	@Deprecated
	public WrappedDataFrame<K, V> toDF(RowMarshaller marshaller) {
		if (wrapped instanceof WrappedDataFrame) return (WrappedDataFrame<K, V>) wrapped;
		JavaRDD<Tuple2<K, V>> rdd = wrapped().jrdd();
		return new WrappedDataFrame<K, V>(new SQLContext(rdd.context()), marshaller, rdd.map(t -> t._2));
	}

	public PairWrapped<K, V> pairWrapped() {
		Wrapped<Tuple2<K, V>> w = wrapped;
		while (PairRDS.class.isAssignableFrom(w.getClass()))
			w = ((PairRDS<K, V>) w).wrapped;
		return w instanceof PairWrapped ? (PairWrapped<K, V>) w : null;
	}

	public static <K, V> PairRDS<K, V> emptyPair(JavaSparkContext sc) {
		return new PairRDS<>(sc);
	}
}
