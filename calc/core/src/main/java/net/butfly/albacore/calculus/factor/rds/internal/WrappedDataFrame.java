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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
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
@Deprecated
@SuppressWarnings("unchecked")
public class WrappedDataFrame<K, V> implements PairWrapped<K, V> {
	private static final long serialVersionUID = 3614737214835144193L;
	protected final transient DataFrame frame;
	private final Class<V> vClass;
	private final RowMarshaller marshaller;

	public WrappedDataFrame(DataFrame frame, RowMarshaller marshaller, Class<V> vClass) {
		this.marshaller = marshaller;
		this.vClass = vClass;
		this.frame = frame;
	}

	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, JavaRDDLike<V, ?> rdd) {
		this(ssc, marshaller, rdd.rdd());
	}

	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, List<V> t) {
		this(ssc, marshaller, ssc.sparkContext().parallelize(JavaConversions.asScalaBuffer(t).seq(), ssc.sparkContext()
				.defaultMinPartitions(), RDSupport.tag()));
	}

	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, RDD<V> rdd) {
		this.marshaller = marshaller;
		vClass = (Class<V>) rdd.elementClassTag().runtimeClass();
		frame = ssc.createDataFrame(rdd, vClass);
	}

	@SafeVarargs
	public WrappedDataFrame(SQLContext ssc, RowMarshaller marshaller, V... t) {
		this(ssc, marshaller, Arrays.asList(t));
	}

	@Override
	public Map<K, V> collectAsMap() {
		return Reflections.transMapping(frame.collectAsList(), row -> new Tuple2<K, V>((K) marshaller.unmarshallId(row), marshaller
				.unmarshall(row, vClass)));
	}

	@Override
	public List<K> collectKeys(Class<K> cls) {
		return frame.map(new ScalarFunc1<Row, K>() {
			private static final long serialVersionUID = -1011796477262995212L;

			@Override
			public K apply(Row row) {
				return (K) row.get(row.fieldIndex(marshaller.parseQualifier(Marshaller.keyField(vClass))));
			}
		}, RDSupport.tag(cls)).toJavaRDD().collect();
	}

	@Override
	public final long count() {
		return rdd().count();
	}

	@Override
	public DStream<Tuple2<K, V>> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> jrdd()).dstream();
	}

	// TODO
	@Override
	public WrappedDataFrame<K, V> filter(Function<Tuple2<K, V>, Boolean> func) {
		return new WrappedDataFrame<>(frame.sqlContext(), marshaller, jrdd().filter(func).map(t -> t._2).rdd());
	}

	@Override
	public PairWrapped<K, V> filter(Function2<K, V, Boolean> func) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final Tuple2<K, V> first() {
		Row row = frame.first();
		return new Tuple2<>((K) marshaller.unmarshallId(row), marshaller.unmarshall(row, vClass));
	}

	@Override
	public void foreach(VoidFunction<Tuple2<K, V>> consumer) {
		jrdd().foreach(consumer);
	}

	@Override
	public void foreach(VoidFunction2<K, V> consumer) {
		// TODO Auto-generated method stub

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
	public int getNumPartitions() {
		return Integer.parseInt(frame.sqlContext().getConf("spark.sql.shuffle.partitions"));
	}

	@Override
	@Deprecated
	public <U> PairWrapped<K, Iterable<V>> groupByKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final boolean isEmpty() {
		return frame.count() > 0;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, V2>> join(Wrapped<Tuple2<K, V2>> other, float ratioPartitions, Class<?>... vClass2) {
		// TODO Auto-generated method stub
		return null;
	};

	@Override
	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, Class<?>... vClass2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V2> PairWrapped<K, Tuple2<V, Optional<V2>>> leftOuterJoin(Wrapped<Tuple2<K, V2>> other, float ratioPartitions,
			Class<?>... vClass2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public final <T1> Wrapped<T1> map(Function<Tuple2<K, V>, T1> func, Class<?>... cls) {
		return new WrappedRDD<T1>(jrdd().map(func).rdd());
	}

	@Override
	public <K2, V2> PairWrapped<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> func, Class<?>... vClass2) {
		return new WrappedDataFrame<>(frame.sqlContext(), marshaller, jrdd().mapToPair(func).map(t -> t._2).rdd());
	}

	@Override
	public WrappedDataFrame<K, V> persist(StorageLevel level) {
		return new WrappedDataFrame<>(frame.persist(level), marshaller, vClass);
	}

	@Override
	public RDD<Tuple2<K, V>> rdd() {
		return frame.map(new ScalarFunc1<Row, Tuple2<K, V>>() {
			private static final long serialVersionUID = -3867553277171014049L;

			@Override
			public Tuple2<K, V> apply(Row row) {
				return new Tuple2<>((K) marshaller.unmarshallId(row), marshaller.unmarshall(row, vClass));
			}
		}, RDSupport.tag());
	}

	@Override
	public Tuple2<K, V> reduce(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> func) {
		return JavaRDD.fromRDD(frame.sqlContext().sparkContext().parallelize(JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(
				func))).seq(), frame.sqlContext().sparkContext().defaultMinPartitions(), classTag()), classTag()).reduce(func);
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
	public WrappedDataFrame<K, V> repartition(float ratio) {
		return new WrappedDataFrame<>(frame.repartition((int) Math.ceil(rdd().getNumPartitions() * ratio)), marshaller, vClass);
	}

	@Override
	public PairWrapped<K, V> repartition(float ratio, boolean rehash) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S> PairWrapped<K, V> sortBy(Function<Tuple2<K, V>, S> comp, Class<?>... cls) {
		JavaRDD<Tuple2<K, V>> v = jrdd().sortBy(comp, true, getNumPartitions());
		return new PairRDS<>(new WrappedRDD<>(v));
	}

	@Override
	public <S> PairWrapped<K, V> sortBy(Function2<K, V, S> comp, Class<?>... cls) {
		JavaRDD<Tuple2<K, V>> v = jrdd().sortBy(t -> comp.call(t._1, t._2), true, getNumPartitions());
		return new PairRDS<>(new WrappedRDD<>(v));
	}

	@Override
	public PairWrapped<K, V> sortByKey(boolean asc) {
		return new WrappedDataFrame<>(frame.sort(marshaller.parseQualifier(Marshaller.keyField(vClass))), marshaller, vClass);
	}

	@Override
	public WrappedDataFrame<K, V> toDF(RowMarshaller marshaller) {
		return this;
	}

	@Override
	public WrappedDataset<K, V> toDS(Class<?>... vClass) {
		return new WrappedDataset<>(frame.as(EncoderBuilder.with(vClass)));
	}

	@Override
	public PairWrapped<K, V> union(Wrapped<Tuple2<K, V>> other) {
		if (other instanceof WrappedDataFrame) return new WrappedDataFrame<>(frame.unionAll(((WrappedDataFrame<K, V>) other).frame),
				marshaller, vClass);
		else return new WrappedDataFrame<>(frame.unionAll(new WrappedDataFrame<>(frame.sqlContext(), marshaller, other.jrdd().map(t -> t._2)
				.rdd()).frame), marshaller, vClass);
	}

	@Override
	public WrappedDataFrame<K, V> unpersist() {
		return new WrappedDataFrame<>(frame.unpersist(), marshaller, vClass);
	}

	@Override
	public PairWrapped<K, V> wrapped() {
		return this;
	}

	@Override
	public List<Tuple2<K, V>> collect() {
		List<Tuple2<K, V>> l = new ArrayList<>();
		for (Row row : frame.collect())
			l.add(new Tuple2<>((K) marshaller.unmarshallId(row), marshaller.unmarshall(row, vClass)));
		return l;
	}
}
