//package net.butfly.albacore.calculus.factor.rds;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Map;
//
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//
//import com.google.common.base.Optional;
//
//import scala.Tuple2;
//
//@Deprecated
//public class JavaPairRDS<K, V> extends JavaRDSBase<Tuple2<K, V>, JavaPairRDD<K, V>, JavaPairDStream<K, V>> implements Serializable {
//	private static final long serialVersionUID = 1908771862271821691L;
//
//	protected JavaPairRDS() {}
//
//	@SafeVarargs
//	public JavaPairRDS(JavaPairRDD<K, V>... pairRDD) {
//		super(pairRDD);
//	}
//
//	public JavaPairRDS(JavaPairDStream<K, V> pairDStream) {
//		super(pairDStream);
//	}
//
//	@SafeVarargs
//	public JavaPairRDS(JavaRDD<Tuple2<K, V>>... rdd) {
//		type = RDSType.RDD;
//		this.rdds = pair(rdd);
//	}
//
//	public JavaPairRDS(JavaDStream<Tuple2<K, V>> dstream) {
//		type = RDSType.DSTREAM;
//		this.dstream = dstream.mapToPair(t -> t);
//	}
//
//	@SafeVarargs
//	public JavaPairRDS(JavaSparkContext sc, Tuple2<K, V>... t) {
//		super(sc, t);
//	}
//
//	public JavaPairRDS(JavaSparkContext sc, Map<K, V> m) {
//		this(sc.parallelize(new ArrayList<>(m.entrySet())).map(e -> new Tuple2<K, V>(e.getKey(), e.getValue())).mapToPair(t -> t));
//	}
//
//	public JavaPairRDS<K, V> folk() {
//		switch (type) {
//		case RDD:
//			for (int i = 0; i < rdds.length; i++)
//				rdds[i] = rdds[i].cache();
//			return new JavaPairRDS<K, V>(rdds);
//		case DSTREAM:
//			dstream = dstream.cache();
//			return new JavaPairRDS<K, V>(dstream);
//		}
//		return this;
//	}
//
//	public JavaPairRDS<K, V> reduceByKey(Function2<V, V, V> func) {
//		switch (type) {
//		case RDD:
//			return new JavaPairRDS<K, V>(each(rdds, rdd -> rdd.reduceByKey(func)));
//		case DSTREAM:
//			return new JavaPairRDS<K, V>(dstream.reduceByKey(func));
//		default:
//			throw new IllegalArgumentException();
//		}
//	}
//
//	public <W> JavaPairRDS<K, Tuple2<V, W>> join(JavaPairRDS<K, W> other) {
//		switch (type) {
//		case RDD:
//			return new JavaPairRDS<K, Tuple2<V, W>>(union(rdds).join(union(other.rdds)));
//		case DSTREAM:
//			return new JavaPairRDS<K, Tuple2<V, W>>(dstream.join(other.dstream));
//		default:
//			throw new IllegalArgumentException();
//		}
//	}
//
//	public <W> JavaPairRDS<K, Tuple2<V, Optional<W>>> leftOuterJoin(JavaPairRDS<K, W> other) {
//		switch (type) {
//		case RDD:
//			return new JavaPairRDS<K, Tuple2<V, Optional<W>>>(union(rdds).leftOuterJoin(union(other.rdds)));
//		case DSTREAM:
//			return new JavaPairRDS<K, Tuple2<V, Optional<W>>>(dstream.leftOuterJoin(other.dstream));
//		default:
//			throw new IllegalArgumentException();
//		}
//	}
//}
