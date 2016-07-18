//package net.butfly.albacore.calculus.factor.rds;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.List;
//
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaRDDLike;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.StreamingContext;
//import org.apache.spark.streaming.dstream.DStream;
//
//import net.butfly.albacore.calculus.Mode;
//import net.butfly.albacore.calculus.streaming.RDDDStream;
//import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;
//import scala.Tuple2;
//import scala.collection.JavaConversions;
//import scala.collection.Seq;
//
///**
// * Wrapper of Dataframe
// * 
// * @author butfly
// *
// * @param <Row>
// */
//public class WSD implements Wrapped<Row> {
//	private static final long serialVersionUID = 3614737214835144193L;
//	final private SparkContext sc;
//	final protected transient DataFrame frame;
//
//	protected WSD(DataFrame frame) {
//		sc = frame.rdd().context();
//		this.frame = frame;
//	}
//
////	public WSD(JavaRDDLike<Row, ?> rdd) {
////		this(rdd.rdd());
////	}
//
//	@SafeVarargs
//	public WSD(SparkContext sc, Row... t) {
//		this(sc, Arrays.asList(t));
//	}
//
//	public WSD(SparkContext sc, List<Row> t) {
//		this(sc.parallelize(JavaConversions.asScalaBuffer(t).seq(), sc.defaultParallelism(), RDSupport.tag()));
//	}
//
//	@Override
//	public Mode mode() {
//		return Mode.STOCKING;
//	}
//
//	@Override
//	public int getNumPartitions() {
//		return rdd.getNumPartitions();
//	}
//
//	@Override
//	public WSD repartition(float ratio) {
//		return new WSD(jrdd().repartition((int) Math.ceil(rdd.getNumPartitions() * ratio)).rdd());
//	}
//
//	@Override
//	public WSD unpersist() {
//		return new WSD(rdd.unpersist(true));
//	}
//
//	@Override
//	public WSD persist() {
//		return new WSD(rdd.persist());
//	}
//
//	@Override
//	public WSD persist(StorageLevel level) {
//		return new WSD(rdd.persist(level));
//	}
//
//	@Override
//	public final boolean isEmpty() {
//		return rdd.isEmpty();
//	}
//
//	@Override
//	public void foreachRDD(VoidFunction<JavaRDD<Row>> consumer) {
//		try {
//			consumer.call(jrdd());
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	@Override
//	public void foreach(VoidFunction<Row> consumer) {
//		jrdd().foreach(consumer);
//	}
//
//	@Override
//	public Wrapped<Row> union(Wrapped<Row> other) {
//		if (RDS.class.isAssignableFrom(other.getClass())) return union(((RDS<Row>) other).wrapped());
//		StreamingContext ssc;
//		if (WSD.class.isAssignableFrom(other.getClass())) return new WSD<>(jrdd().union(other.jrdd()));
//		else if (WStream.class.isAssignableFrom(other.getClass())) {
//			ssc = ((WStream<Row>) other).ssc;
//			return new WStream<>(jdstream(ssc).union(other.jdstream(ssc)));
//		} else throw new IllegalArgumentException();
//	}
//
//	@Override
//	public WSD filter(Function<Row, Boolean> func) {
//		return new WSD(jrdd().filter(func).rdd());
//	}
//
//	@Override
//	public <K2, V2> WSD mapToPair(PairFunction<Row, K2, V2> func) {
//		return new WSD(jrdd().mapToPair(func).rdd());
//	}
//
//	@Override
//	public final <T1> WSD<T1> map(Function<Row, T1> func) {
//		return new WSD<T1>(jrdd().map(func).rdd());
//	}
//
//	@Override
//	public final Row first() {
//		return rdd.first();
//	}
//
//	@Override
//	public final Row reduce(Function2<Row, Row, Row> func) {
//		Seq<Row> seq = JavaConversions.asScalaBuffer(Arrays.asList(jrdd().reduce(func))).seq();
//		return JavaRDD.fromRDD(sc.parallelize(seq, sc.defaultMinPartitions(), classTag()), classTag()).reduce(func);
//	}
//
//	@Override
//	public final long count() {
//		return rdd.count();
//	}
//
//	@Override
//	public DStream<Row> dstream(StreamingContext ssc) {
//		return RDDDStream.stream(ssc, Mechanism.CONST, () -> JavaRDD.fromRDD(rdd(), classTag())).dstream();
//	};
//
//	@Override
//	public RDD<Row> rdd() {
//		return frame.rdd();
//	}
//
//	@Override
//	public Collection<RDD<Row>> rdds() {
//		return Arrays.asList(rdd);
//	}
//
//	@Override
//	public <S> WSD sortBy(Function<Row, S> comp) {
//		JavaRDD<Row> rdd = JavaRDD.fromRDD(rdd(), classTag());
//		return new WSD(rdd.sortBy(comp, true, rdd.getNumPartitions()));
//	}
//
//	@Override
//	public Wrapped<Row> wrapped() {
//		return this;
//	}
//}
