package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.datasource.DataDetail;
import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.factor.modifier.Key;
import net.butfly.albacore.calculus.factor.rds.PairRDS;
import net.butfly.albacore.calculus.marshall.Marshaller;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

/**
 * Wrapper of Dataframe
 * 
 * @author butfly
 *
 */
public class WrappedDataset<K, V> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 3614737214835144193L;
	final protected transient Dataset<V> dataset;

	public WrappedDataset(Dataset<V> dataset) {
		this.dataset = dataset;
	}

	public WrappedDataset(SQLContext ssc, RDD<V> rdd) {
		dataset = ssc.createDataset(rdd, Encoders.kryo(rdd.elementClassTag()));
	}

	public WrappedDataset(SQLContext ssc, JavaRDDLike<V, ?> rdd) {
		this(ssc, rdd.rdd());
	}

	@SafeVarargs
	public WrappedDataset(SQLContext ssc, V... t) {
		this(ssc, Arrays.asList(t));
	}

	public WrappedDataset(SQLContext ssc, List<V> t) {
		this(ssc, ssc.sparkContext().parallelize(JavaConversions.asScalaBuffer(t).seq(), ssc.sparkContext().defaultMinPartitions(),
				RDSupport.tag()));
	}

	@Override
	public Mode mode() {
		return Mode.STOCKING;
	}

	@Override
	public int getNumPartitions() {
		return dataset.rdd().getNumPartitions();
	}

	@Override
	public WrappedDataset<K, V> repartition(float ratio) {
		return new WrappedDataset<K, V>(dataset.repartition((int) Math.ceil(rdd().getNumPartitions() * ratio)));
	}

	@Override
	public WrappedDataset<K, V> unpersist() {
		return new WrappedDataset<K, V>(dataset.unpersist());
	}

	@Override
	public WrappedDataset<K, V> persist() {
		return new WrappedDataset<K, V>(dataset.persist());
	}

	@Override
	public WrappedDataset<K, V> persist(StorageLevel level) {
		return persist();
	}

	@Override
	public final boolean isEmpty() {
		return dataset.count() > 0;
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<Tuple2<K, V>>> consumer) {
		try {
			consumer.call(jrdd());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void foreach(VoidFunction<Tuple2<K, V>> consumer) {
		jrdd().foreach(consumer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other) {
		if (other instanceof WrappedDataset) return new WrappedDataset<>(dataset.union(((WrappedDataset<K, V>) other).dataset));
		else return new WrappedDataset<>(dataset.union(new WrappedDataset<K, V>(dataset.sqlContext(), mapWithKey(other.jrdd())).dataset));
	}

	private static <KK, TT> RDD<TT> mapWithKey(JavaRDD<Tuple2<KK, TT>> jrdd) {
		return jrdd.map(t -> t._2).rdd();
	}

	private static <KK, TT> RDD<TT> mapWithKey(JavaPairRDD<KK, TT> jrdd) {
		return jrdd.map(t -> t._2).rdd();
	}

	@Override
	public WrappedDataset<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		return new WrappedDataset<>(dataset.sqlContext(), mapWithKey(jrdd().filter(func)));
	}

	@Override
	public <K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		return new WrappedDataset<>(dataset.sqlContext(), mapWithKey(jrdd().mapToPair(func)));
	}

	@Override
	public final <T1> Wrapped<T1> map(Function<Tuple2<K, V>, T1> func) {
		return new WrappedRDD<T1>(jrdd().map(func).rdd());
	}

	@Override
	public final Tuple2<K, V> first() {
		V v = dataset.first();
		return new Tuple2<>(key(v), v);
	}

	@Override
	public Tuple2<K, V> reduce(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> func) {
		return JavaRDD.fromRDD(
				dataset.sqlContext().sparkContext().parallelize(JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(func))).seq(),
						dataset.sqlContext().sparkContext().defaultMinPartitions(), classTag()),
				classTag()).reduce(func);
	}

	@Override
	public final long count() {
		return rdd().count();
	}

	@Override
	public DStream<Tuple2<K, V>> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> jrdd()).dstream();
	};

	@Override
	public RDD<Tuple2<K, V>> rdd() {
		ClassTag<K> kt = RDSupport.tag();
		ClassTag<V> vt = RDSupport.tag();
		return dataset.map(new AbstractFunction1<V, Tuple2<K, V>>() {
			@Override
			public Tuple2<K, V> apply(V v) {
				return new Tuple2<>(key(v), v);
			}
		}, Encoders.tuple(Encoders.kryo(kt), Encoders.kryo(vt))).rdd();
	}

	@Override
	public Collection<RDD<Tuple2<K, V>>> rdds() {
		return Arrays.asList(rdd());
	}

	@Override
	public <S> PairWrapped<K, V> sortBy(Function<Tuple2<K, V>, S> comp) {
		JavaRDD<Tuple2<K, V>> v = jrdd().sortBy(comp, true, getNumPartitions());
		return new PairRDS<>(new WrappedRDD<>(v));
	}

	@Override
	public PairWrapped<K, V> wrapped() {
		return this;
	}

	@Override
	public <RK, RV, WK, WV> void save(DataSource<K, RK, RV, WK, WV> ds, DataDetail<V> dd) {
		throw new NotImplementedException();
	}

	@Override
	public Map<K, V> collectAsMap() {
		return Reflections.transMapping(dataset.collectAsList(), v -> new Tuple2<K, V>(key(v), v));
	}

	@Override
	public List<K> collectKeys() {
		return Reflections.transform(dataset.collectAsList(), v -> key(v));
	}

	@Override
	public void foreachPairRDD(VoidFunction<JavaPairRDD<K, V>> consumer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void foreach(VoidFunction2<K, V> consumer) {
		// TODO Auto-generated method stub

	}

	@Override
	public <U> PairWrapped<K, Iterable<V>> groupByKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PairWrapped<K, V> reduceByKey(Function2<V, V, V> func) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PairWrapped<K, V> reduceByKey(Function2<V, V, V> func, float ratioPartitions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, int numPartitions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaPairRDD<K, V> pairRDD() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<JavaPairRDD<K, V>> pairRDDs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PairWrapped<K, V> sortByKey(boolean asc) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S> PairWrapped<K, V> sortBy(Function2<K, V, S> comp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K maxKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K minKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PairWrapped<K, V> filter(Function2<K, V, Boolean> func) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PairWrapped<K, V> repartition(float ratio, boolean rehash) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	private static <K, V> K key(V v) {
		try {
			return null == v ? null : (K) Marshaller.parse(v.getClass(), Key.class)._1.get(v);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			return null;
		}
	}
}
