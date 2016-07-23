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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
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
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
import net.butfly.albacore.calculus.utils.Reflections;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

/**
 * Wrapper of Dataframe
 * 
 * @author butfly
 *
 * @param <Row>
 */
@SuppressWarnings("unchecked")
public class WrappedDataFrame<K, V> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 3614737214835144193L;
	final private Class<K> kClass;
	final private Class<V> vClass;
	final protected transient DataFrame frame;
	private final RowMarshaller marshaller;

	public WrappedDataFrame(DataFrame frame, RowMarshaller marshaller, Class<V> vClass) {
		this.marshaller = marshaller;
		this.kClass = (Class<K>) Marshaller.parse(vClass, Key.class)._1.getType();
		this.vClass = vClass;
		this.frame = frame;
	}

	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, RDD<V> rdd) {
		this.marshaller = marshaller;
		vClass = (Class<V>) rdd.elementClassTag().runtimeClass();
		kClass = (Class<K>) Marshaller.parse(vClass, Key.class)._1.getType();
		frame = ssc.createDataFrame(rdd, vClass);
	}

	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, JavaRDDLike<V, ?> rdd) {
		this(ssc, marshaller, rdd.rdd());
	}

	@SafeVarargs
	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, V... t) {
		this(ssc, marshaller, Arrays.asList(t));
	}

	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, List<V> t) {
		this(ssc, marshaller, ssc.sparkContext().parallelize(JavaConversions.asScalaBuffer(t).seq(),
				ssc.sparkContext().defaultMinPartitions(), RDSupport.tag()));
	}

	@Override
	public Mode mode() {
		return Mode.STOCKING;
	}

	@Override
	public int getNumPartitions() {
		return frame.rdd().getNumPartitions();
	}

	@Override
	public WrappedDataFrame<K, V> repartition(float ratio) {
		return new WrappedDataFrame<K, V>(frame.repartition((int) Math.ceil(rdd().getNumPartitions() * ratio)), marshaller, vClass);
	}

	@Override
	public WrappedDataFrame<K, V> unpersist() {
		return new WrappedDataFrame<K, V>(frame.unpersist(), marshaller, vClass);
	}

	@Override
	public WrappedDataFrame<K, V> persist() {
		return new WrappedDataFrame<K, V>(frame.persist(), marshaller, vClass);
	}

	@Override
	public WrappedDataFrame<K, V> persist(StorageLevel level) {
		return persist();
	}

	@Override
	public final boolean isEmpty() {
		return frame.count() > 0;
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

	@Override
	public PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other) {
		if (other instanceof WrappedDataFrame)
			return new WrappedDataFrame<>(frame.unionAll(((WrappedDataFrame<K, V>) other).frame), marshaller, vClass);
		else return new WrappedDataFrame<>(
				frame.unionAll(new WrappedDataFrame<K, V>(frame.sqlContext(), marshaller, mapWithKey(other.jrdd())).frame), marshaller,
				vClass);
	}

	private static <KK, TT> RDD<TT> mapWithKey(JavaRDD<Tuple2<KK, TT>> jrdd) {
		return jrdd.map(t -> t._2).rdd();
	}

	private static <KK, TT> RDD<TT> mapWithKey(JavaPairRDD<KK, TT> jrdd) {
		return jrdd.map(t -> t._2).rdd();
	}

	// TODO
	@Override
	public WrappedDataFrame<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		return new WrappedDataFrame<>(frame.sqlContext(), marshaller, mapWithKey(jrdd().filter(func)));
	}

	@Override
	public <K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func) {
		return new WrappedDataFrame<>(frame.sqlContext(), marshaller, mapWithKey(jrdd().mapToPair(func)));
	}

	@Override
	public final <T1> Wrapped<T1> map(Function<Tuple2<K, V>, T1> func) {
		return new WrappedRDD<T1>(jrdd().map(func).rdd());
	}

	@Override
	public final Tuple2<K, V> first() {
		Row row = frame.first();
		return new Tuple2<>((K) marshaller.unmarshallId(row), marshaller.unmarshall(row, vClass));
	}

	@Override
	public Tuple2<K, V> reduce(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> func) {
		return JavaRDD.fromRDD(
				frame.sqlContext().sparkContext().parallelize(JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(func))).seq(),
						frame.sqlContext().sparkContext().defaultMinPartitions(), classTag()),
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
		return frame.map(new AbstractFunction1<Row, Tuple2<K, V>>() {
			@Override
			public Tuple2<K, V> apply(Row row) {
				return new Tuple2<>((K) marshaller.unmarshallId(row), marshaller.unmarshall(row, vClass));
			}
		}, RDSupport.tag());
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
		return Reflections.transMapping(frame.collectAsList(),
				row -> new Tuple2<K, V>((K) marshaller.unmarshallId(row), marshaller.unmarshall(row, vClass)));
	}

	@Override
	public List<K> collectKeys() {
		return Reflections.transform(frame.collectAsList(), row -> (K) marshaller.unmarshallId(row));
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
}
