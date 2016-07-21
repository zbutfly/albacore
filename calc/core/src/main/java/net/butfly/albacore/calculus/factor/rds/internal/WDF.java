package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import net.butfly.albacore.calculus.streaming.RDDDStream;
import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
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
public class WDF<T> implements Wrapped<T> {
	private static final long serialVersionUID = 3614737214835144193L;
	final private SQLContext ssc;
	final private RowMarshaller marshaller;
	final Class<T> eclass;
	final protected transient DataFrame frame;

	public WDF(DataFrame frame, Class<T> entityClass) {
		ssc = frame.sqlContext();
		marshaller = new RowMarshaller();
		eclass = entityClass;
		this.frame = frame;
	}

	@SuppressWarnings("unchecked")
	public WDF(SQLContext ssc, RDD<T> rdd) {
		this.ssc = ssc;
		marshaller = new RowMarshaller();
		eclass = (Class<T>) rdd.elementClassTag().runtimeClass();
		frame = ssc.createDataFrame(rdd, eclass);
	}

	public WDF(SQLContext ssc, RowMarshaller marshaller, JavaRDDLike<T, ?> rdd) {
		this(ssc, rdd.rdd());
	}

	@SafeVarargs
	public WDF(SQLContext sc, T... t) {
		this(sc, Arrays.asList(t));
	}

	public WDF(SQLContext sc, List<T> t) {
		this(sc, sc.sparkContext().parallelize(JavaConversions.asScalaBuffer(t).seq(), sc.sparkContext().defaultMinPartitions(),
				RDSupport.tag()));
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
	public WDF<T> repartition(float ratio) {
		return new WDF<T>(frame.repartition((int) Math.ceil(rdd().getNumPartitions() * ratio)), eclass);
	}

	@Override
	public WDF<T> unpersist() {
		return new WDF<T>(frame.unpersist(), eclass);
	}

	@Override
	public WDF<T> persist() {
		return new WDF<T>(frame.persist(), eclass);
	}

	@Override
	public WDF<T> persist(StorageLevel level) {
		return persist();
	}

	@Override
	public final boolean isEmpty() {
		return frame.count() > 0;
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<T>> consumer) {
		try {
			consumer.call(jrdd());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void foreach(VoidFunction<T> consumer) {
		jrdd().foreach(consumer);
	}

	@Override
	public Wrapped<T> union(Wrapped<T> other) {
		return new WDF<>(frame.unionAll(new WDF<T>(ssc, other.rdd()).frame), eclass);
	}

	@Override
	public WDF<T> filter(Function<T, Boolean> func) {
		return new WDF<>(ssc, jrdd().filter(func).rdd());
	}

	@Override
	public <K2, V2> Wrapped<Tuple2<K2, V2>> mapToPair(PairFunction<T, K2, V2> func) {
		return new WDF<>(ssc, jrdd().mapToPair(func).rdd());
	}

	@Override
	public final <T1> Wrapped<T1> map(Function<T, T1> func) {
		return new WDF<>(ssc, jrdd().map(func).rdd());
	}

	@Override
	public final T first() {
		return marshaller.unmarshall(frame.rdd().first(), eclass);
	}

	@Override
	public final T reduce(Function2<T, T, T> func) {
		return JavaRDD.fromRDD(ssc.sparkContext().parallelize(JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(func))).seq(),
				ssc.sparkContext().defaultMinPartitions(), classTag()), classTag()).reduce(func);
	}

	@Override
	public final long count() {
		return rdd().count();
	}

	@Override
	public DStream<T> dstream(StreamingContext ssc) {
		return RDDDStream.stream(ssc, Mechanism.CONST, () -> jrdd()).dstream();
	};

	@Override
	public RDD<T> rdd() {
		return frame.map(new AbstractFunction1<Row, T>() {
			@Override
			public T apply(Row row) {
				return marshaller.unmarshall(row, eclass);
			}
		}, RDSupport.tag());
	}

	@Override
	public Collection<RDD<T>> rdds() {
		return Arrays.asList(rdd());
	}

	@Override
	public <S> Wrapped<T> sortBy(Function<T, S> comp) {
		return new WDD<>(jrdd().sortBy(comp, true, getNumPartitions()));
	}

	@Override
	public Wrapped<T> wrapped() {
		return this;
	}
}
