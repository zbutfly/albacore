package net.butfly.albacore.calculus.streaming;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class JavaWrappedPairDStream<K, V> extends JavaPairDStream<K, V>
		implements JavaDStreamLike<Tuple2<K, V>, JavaPairDStream<K, V>, JavaPairRDD<K, V>> {
	private static final long serialVersionUID = -28621241706320035L;
	public JavaStreamingContext jssc;
	protected JavaPairRDD<K, V> rdd;

	public JavaWrappedPairDStream(JavaStreamingContext ssc, JavaPairRDD<K, V> rdd) {
		super(null, rdd.kClassTag(), rdd.vClassTag());
		this.rdd = rdd;
		this.jssc = ssc;
	}

	public RDD<Tuple2<K, V>> rdd() {
		return rdd.rdd();
	}

	@Override
	public DStream<Tuple2<K, V>> dstream() {
		return null;
	}

	@Override
	public DStream<Tuple2<K, V>> checkpoint(Duration arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ClassTag<Tuple2<K, V>> classTag() {
		return rdd.classTag();
	}

	@Override
	public StreamingContext context() {
		return jssc.ssc();
	}

	@Override
	public JavaDStream<Long> count() {
		return RDDDStream.stream(jssc, rdd.count());
	}

	@Override
	public JavaPairDStream<Tuple2<K, V>, Long> countByValue() {
		return streaming(jssc, rdd.countByValue());
	}

	@Override
	public JavaPairDStream<Tuple2<K, V>, Long> countByValue(int arg0) {
		return countByValue();
	}

	@Override
	public JavaPairDStream<Tuple2<K, V>, Long> countByValueAndWindow(Duration arg0, Duration arg1) {
		return countByValue();
	}

	@Override
	public JavaPairDStream<Tuple2<K, V>, Long> countByValueAndWindow(Duration arg0, Duration arg1, int arg2) {
		return countByValue();
	}

	@Override
	public JavaDStream<Long> countByWindow(Duration arg0, Duration arg1) {
		return count();
	}

	@Override
	@Deprecated
	public void foreach(Function<JavaPairRDD<K, V>, Void> arg0) {
		foreachRDD(arg0);
	}

	@Override
	@Deprecated
	public void foreach(Function2<JavaPairRDD<K, V>, Time, Void> arg0) {
		foreachRDD(arg0);
	}

	@Override
	public void foreachRDD(Function<JavaPairRDD<K, V>, Void> arg0) {
		try {
			arg0.call(rdd);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void foreachRDD(Function2<JavaPairRDD<K, V>, Time, Void> arg0) {
		try {
			arg0.call(rdd, new Time(new Date().getTime()));
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void foreachRDD(VoidFunction<JavaPairRDD<K, V>> arg0) {
		try {
			arg0.call(rdd);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void foreachRDD(VoidFunction2<JavaPairRDD<K, V>, Time> arg0) {
		try {
			arg0.call(rdd, new Time(new Date().getTime()));
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public <U> JavaDStream<U> flatMap(FlatMapFunction<Tuple2<K, V>, U> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> flatMapToPair(PairFlatMapFunction<Tuple2<K, V>, K2, V2> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaDStream<List<Tuple2<K, V>>> glom() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R> JavaDStream<R> map(Function<Tuple2<K, V>, R> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U> JavaDStream<U> mapPartitions(FlatMapFunction<Iterator<Tuple2<K, V>>, U> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Tuple2<K, V>>, K2, V2> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void print() {
		// TODO Auto-generated method stub

	}

	@Override
	public void print(int arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public JavaDStream<Tuple2<K, V>> reduce(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DStream<Tuple2<K, V>> reduceByWindow(scala.Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> arg0, Duration arg1,
			Duration arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaDStream<Tuple2<K, V>> reduceByWindow(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> arg0, Duration arg1,
			Duration arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaDStream<Tuple2<K, V>> reduceByWindow(Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> arg0,
			Function2<Tuple2<K, V>, Tuple2<K, V>, Tuple2<K, V>> arg1, Duration arg2, Duration arg3) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaDStream<Long> scalaIntToJavaLong(DStream<Object> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<JavaPairRDD<K, V>> slice(Time arg0, Time arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U> JavaDStream<U> transform(Function<JavaPairRDD<K, V>, JavaRDD<U>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U> JavaDStream<U> transform(Function2<JavaPairRDD<K, V>, Time, JavaRDD<U>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> transformToPair(Function<JavaPairRDD<K, V>, JavaPairRDD<K2, V2>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> transformToPair(Function2<JavaPairRDD<K, V>, Time, JavaPairRDD<K2, V2>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U, W> JavaDStream<W> transformWith(JavaDStream<U> arg0, Function3<JavaPairRDD<K, V>, JavaRDD<U>, Time, JavaRDD<W>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2, W> JavaDStream<W> transformWith(JavaPairDStream<K2, V2> arg0,
			Function3<JavaPairRDD<K, V>, JavaPairRDD<K2, V2>, Time, JavaRDD<W>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U, K2, V2> JavaPairDStream<K2, V2> transformWithToPair(JavaDStream<U> arg0,
			Function3<JavaPairRDD<K, V>, JavaRDD<U>, Time, JavaPairRDD<K2, V2>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2, K3, V3> JavaPairDStream<K3, V3> transformWithToPair(JavaPairDStream<K2, V2> arg0,
			Function3<JavaPairRDD<K, V>, JavaPairRDD<K2, V2>, Time, JavaPairRDD<K3, V3>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaPairRDD<K, V> wrapRDD(RDD<Tuple2<K, V>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	private static <K, V> JavaPairDStream<K, V> streaming(JavaStreamingContext jssc, List<Tuple2<K, V>> r) {
		return new JavaWrappedPairDStream<K, V>(jssc, jssc.sparkContext().parallelize(r).mapToPair(t -> t));
	}

	private static <K, V> JavaPairDStream<K, V> streaming(JavaStreamingContext jssc, Map<K, V> m) {
		List<Tuple2<K, V>> r = new ArrayList<>(m.size());
		for (Map.Entry<K, V> e : m.entrySet())
			r.add(new Tuple2<K, V>(e.getKey(), e.getValue()));
		return streaming(jssc, r);
	}
}