package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;

import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.lambda.ScalarFunc1;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.collection.JavaConversions;

/**
 * Wrapper of Dataframe
 * 
 * @author butfly
 *
 */
@SuppressWarnings("unchecked")
public class WrappedDataset<K, V> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 1736537786999647143L;
	protected final transient Dataset<V> dataset;
	private final Class<V> vClass;

	public WrappedDataset(Dataset<V> dataset) {
		this.vClass = (Class<V>) dataset.unresolvedTEncoder().clsTag().runtimeClass();
		this.dataset = dataset;
	}

	public WrappedDataset(DataFrame frame, Class<?>... vClass) {
		Encoder<V> e = EncoderBuilder.with(vClass);
		this.vClass = (Class<V>) e.clsTag().runtimeClass();
		dataset = frame.as(e);
	}

	public WrappedDataset(SQLContext ssc, JavaRDDLike<V, ?> rdd, Encoder<V> enc) {
		this(ssc, rdd.rdd(), enc);
	}

	public WrappedDataset(SQLContext ssc, List<V> t, Encoder<V> enc) {
		this(ssc, ssc.sparkContext().parallelize(JavaConversions.asScalaBuffer(t).seq(), ssc.sparkContext().defaultMinPartitions(), enc
				.clsTag()), enc);
	}

	public WrappedDataset(SQLContext ssc, RDD<V> rdd, Encoder<V> enc) {
		this.vClass = (Class<V>) enc.clsTag().runtimeClass();
		dataset = ssc.createDataset(rdd, enc);
	}

	@SafeVarargs
	public WrappedDataset(SQLContext ssc, Encoder<V> enc, V... t) {
		this(ssc, Arrays.asList(t), enc);
	}

	@Override
	public Map<K, V> collectAsMap() {
		return Reflections.transMapping(dataset.collectAsList(), v -> new Tuple2<K, V>(Marshaller.key(v), v));
	}

	@Override
	public List<K> collectKeys(Class<K> kClass) {
		Encoder<K> e = EncoderBuilder.with(kClass);
		return dataset.map(Marshaller::key, e).collectAsList();
	}

	@Override
	public final long count() {
		return rdd().count();
	}

	@Override
	public DStream<Tuple2<K, V>> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> jrdd()).dstream();
	}

	@Override
	public WrappedDataset<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		return new WrappedDataset<>(dataset.filter(value -> func.call(new Tuple2<>(Marshaller.key(value), value))));
	}

	@Override
	public PairWrapped<K, V> filter(Function2<K, V, Boolean> func) {
		return new WrappedDataset<>(dataset.filter(v -> func.call(Marshaller.key(v), v)));
	}

	@Override
	public final Tuple2<K, V> first() {
		V v = dataset.first();
		return new Tuple2<>(Marshaller.key(v), v);
	}

	@Override
	public void foreach(VoidFunction2<K, V> consumer) {
		dataset.foreach(v -> consumer.call(Marshaller.key(v), v));
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<Tuple2<K, V>>> consumer) {
		try {
			consumer.call(jrdd());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// XXX:??
	@Override
	public int getNumPartitions() {
		return Integer.parseInt(dataset.sqlContext().getConf("spark.sql.shuffle.partitions"));
	}

	@Override
	@Deprecated
	public PairWrapped<K, Iterable<V>> groupByKey() {
		return new PairRDS<>(new WrappedRDD<>(pairRDD().groupByKey()));
	}

	@Override
	public final boolean isEmpty() {
		return dataset.count() > 0;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2) {
		Column col1 = dataset.toDF().col(Marshaller.keyField(vClass).getName());
		Wrapped<Tuple2<K, V2>> w = other.wrapped();
		if (w instanceof WrappedDataset) {
			WrappedDataset<K, V2> ds2 = (WrappedDataset<K, V2>) other.wrapped();
			Dataset<Tuple2<V, V2>> ds = dataset.joinWith(ds2.dataset, col1.equalTo(ds2.dataset.toDF().col(Marshaller.keyField(ds2.vClass)
					.getName())));
			return new PairRDS<>(new WrappedRDD<>(ds.rdd().toJavaRDD().map(t -> new Tuple2<K, Tuple2<V, V2>>(Marshaller.key(t._1), t))
					.rdd()));
		} else return join(new WrappedDataset<K, V2>(dataset.sqlContext(), other.jrdd().map(t -> t._2), EncoderBuilder.with(vClass2)),
				vClass2);
	};

	@Override
	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions, Class<?>... vClass2) {
		return repartition(ratioPartitions).join(other.repartition(ratioPartitions), vClass2);
	}

	private Encoder<K> keyEncoder() {
		return EncoderBuilder.with((Class<K>) Marshaller.keyField(vClass).getType());
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2) {
		Column col1 = dataset.toDF().col(Marshaller.keyField(vClass).getName());
		Wrapped<Tuple2<K, V2>> w = other.wrapped();
		if (w instanceof WrappedDataset) {
			WrappedDataset<K, V2> ds2 = (WrappedDataset<K, V2>) other.wrapped();
			Dataset<Tuple2<V, V2>> ds = dataset.joinWith(ds2.dataset, col1.equalTo(ds2.dataset.toDF().col(Marshaller.keyField(ds2.vClass)
					.getName())), "left_outer");
			return new PairRDS<>(new WrappedRDD<>(ds.rdd().toJavaRDD().map(t -> new Tuple2<K, Tuple2<V, Optional<V2>>>(Marshaller.key(t._1),
					new Tuple2<>(t._1, Optional.fromNullable(t._2)))).rdd()));
		} else return leftOuterJoin(new WrappedDataset<K, V2>(dataset.sqlContext(), other.jrdd().map(t -> t._2), EncoderBuilder.with(
				vClass2)), vClass2);
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, float ratioPartitions,
			Class<?>... vClass2) {
		return repartition(ratioPartitions).leftOuterJoin(other.repartition(ratioPartitions), vClass2);
	}

	@Override
	public final <T1> Wrapped<T1> map(Function<Tuple2<K, V>, T1> func, Class<?>... vClass2) {
		return new WrappedRDD<T1>(jrdd().map(func).rdd());
	}

	@Override
	public <K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func, Class<?>... vClass2) {
		// key changing cause key lost
		return new PairRDS<K2, V2>(new WrappedRDD<>(jrdd().mapToPair(func)));
		// return new WrappedDataset<K2, V2>(dataset.map(v -> func.call(new
		// Tuple2<>(Marshaller.key(v), v))._2, EncoderBuilder.with(vClass2)));
	}

	@Override
	public WrappedDataset<K, V> persist(StorageLevel level) {
		if (null == level || StorageLevel.NONE().equals(level)) return this;
		return new WrappedDataset<K, V>(dataset.persist(level));
	}

	@Override
	public RDD<Tuple2<K, V>> rdd() {
		return dataset.map(new ScalarFunc1<V, Tuple2<K, V>>() {
			private static final long serialVersionUID = 8451802808070166668L;

			@Override
			public Tuple2<K, V> apply(V v) {
				return new Tuple2<>(Marshaller.key(v), v);
			}
		}, Encoders.tuple(keyEncoder(), dataset.unresolvedTEncoder())).repartition(getNumPartitions()).rdd();
		// XXX OOM!!
	}

	@Override
	public Tuple2<K, V> reduce(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> func) {
		V v = dataset.reduce((v1, v2) -> func.call(new Tuple2<K, V>(Marshaller.key(v1), v1), new Tuple2<K, V>(Marshaller.key(v2), v2))._2);
		return new Tuple2<K, V>(Marshaller.key(v), v);
	}

	@Override
	public PairWrapped<K, V> reduceByKey(Function2<V, V, V> func) {
		return new WrappedDataset<>(dataset.groupBy(new ScalarFunc1<V, K>() {
			private static final long serialVersionUID = 4018024923953862948L;

			@Override
			public K apply(V v) {
				return Marshaller.key(v);
			}
		}, keyEncoder()).mapGroups((key, values) -> {
			V v0 = null;
			if (values.hasNext()) v0 = values.next();
			while (values.hasNext())
				v0 = func.call(v0, values.next());
			return v0;
		}, dataset.unresolvedTEncoder()));
	}

	@Override
	public PairWrapped<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions) {
		return new WrappedDataset<>(dataset.groupBy(new ScalarFunc1<V, K>() {
			private static final long serialVersionUID = 4018024923953862948L;

			@Override
			public K apply(V v) {
				return Marshaller.key(v);
			}
		}, keyEncoder()).mapGroups((key, values) -> {
			V v0 = null;
			if (values.hasNext()) v0 = values.next();
			while (values.hasNext())
				v0 = func.call(v0, values.next());
			return v0;
		}, dataset.unresolvedTEncoder()));
	}

	@Override
	public WrappedDataset<K, V> repartition(float ratio) {
		return new WrappedDataset<K, V>(dataset.repartition((int) Math.ceil(getNumPartitions() * ratio)));
	}

	@Override
	public <S> PairWrapped<K, V> sortBy(Function<Tuple2<K, V>, S> comp, Class<?>... vClass2) {
		JavaRDD<Tuple2<K, V>> v = jrdd().sortBy(comp, true, getNumPartitions());
		return new PairRDS<>(new WrappedRDD<>(v));
	}

	@Override
	public <S> PairWrapped<K, V> sortBy(Function2<K, V, S> comp, Class<?>... vClass2) {
		JavaRDD<Tuple2<K, V>> v = jrdd().sortBy(t -> comp.call(t._1, t._2), true, getNumPartitions());
		return new PairRDS<>(new WrappedRDD<>(v));
	}

	@Override
	public PairWrapped<K, V> sortByKey(boolean asc) {
		DataFrame df = dataset.toDF();
		Column col = df.col(Marshaller.keyField(vClass).getName());
		return new WrappedDataset<>(df.sort(asc ? col.asc() : col.desc()), vClass);
	}

	@Override
	@Deprecated
	public WrappedDataFrame<K, V> toDF(RowMarshaller marshaller) {
		return new WrappedDataFrame<>(dataset.toDF(), marshaller, vClass);
	}

	@Override
	public WrappedDataset<K, V> toDS(Class<?>... vClass) {
		return this;
	}

	@Override
	public PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other) {
		Wrapped<Tuple2<K, V>> w = other.wrapped();
		return w instanceof WrappedDataset ? new WrappedDataset<>(dataset.union(((WrappedDataset<K, V>) other.wrapped()).dataset))
				: union(new WrappedDataset<K, V>(dataset.sqlContext(), other.jrdd().map(t -> t._2), dataset.unresolvedTEncoder()));

	}

	@Override
	public WrappedDataset<K, V> unpersist() {
		return new WrappedDataset<K, V>(dataset.unpersist());
	}

	@Override
	public PairWrapped<K, V> wrapped() {
		return this;
	}

	@Override
	public List<Tuple2<K, V>> collect() {
		List<Tuple2<K, V>> l = new ArrayList<>();
		foreach(t -> l.add(t));
		return l;
	}
}
