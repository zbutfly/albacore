package net.butfly.albacore.calculus.streaming;

import java.util.ArrayList;
import java.util.Arrays;
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

public class JavaWrappedDStream<T> extends JavaDStream<T> implements JavaDStreamLike<T, JavaDStream<T>, JavaRDD<T>> {
	private static final long serialVersionUID = -28621241706320035L;
	public JavaStreamingContext jssc;
	protected JavaRDD<T> rdd;

	public JavaWrappedDStream(JavaStreamingContext ssc, JavaRDD<T> rdd) {
		super(null, rdd.classTag());
		this.rdd = rdd;
		this.jssc = ssc;
	}

	public RDD<T> rdd() {
		return rdd.rdd();
	}

	@Override
	public DStream<T> dstream() {
		return null;
	}

	@Override
	public DStream<T> checkpoint(Duration arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ClassTag<T> classTag() {
		return rdd.classTag();
	}

	@Override
	public StreamingContext context() {
		return jssc.ssc();
	}

	@Override
	public JavaDStream<Long> count() {
		return streaming(jssc, rdd.count());
	}

	@Override
	public JavaPairDStream<T, Long> countByValue() {
		return streaming(jssc, rdd.countByValue());
	}

	@Override
	public JavaPairDStream<T, Long> countByValue(int arg0) {
		return countByValue();
	}

	@Override
	public JavaPairDStream<T, Long> countByValueAndWindow(Duration arg0, Duration arg1) {
		return countByValue();
	}

	@Override
	public JavaPairDStream<T, Long> countByValueAndWindow(Duration arg0, Duration arg1, int arg2) {
		return countByValue();
	}

	@Override
	public JavaDStream<Long> countByWindow(Duration arg0, Duration arg1) {
		return count();
	}

	@Override
	public <U> JavaDStream<U> flatMap(FlatMapFunction<T, U> arg0) {
		return new JavaWrappedDStream<>(jssc, rdd.flatMap(arg0));
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> flatMapToPair(PairFlatMapFunction<T, K2, V2> arg0) {
		return new JavaWrappedPairDStream<K2, V2>(jssc, rdd.flatMapToPair(arg0));
	}

	@Override
	@Deprecated
	public void foreach(Function<JavaRDD<T>, Void> arg0) {
		foreachRDD(arg0);
	}

	@Override
	@Deprecated
	public void foreach(Function2<JavaRDD<T>, Time, Void> arg0) {
		foreachRDD(arg0);
	}

	@Override
	public void foreachRDD(Function<JavaRDD<T>, Void> arg0) {
		try {
			arg0.call(rdd);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void foreachRDD(Function2<JavaRDD<T>, Time, Void> arg0) {
		try {
			arg0.call(rdd, new Time(new Date().getTime()));
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void foreachRDD(VoidFunction<JavaRDD<T>> arg0) {
		try {
			arg0.call(rdd);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void foreachRDD(VoidFunction2<JavaRDD<T>, Time> arg0) {
		try {
			arg0.call(rdd, new Time(new Date().getTime()));
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public JavaDStream<List<T>> glom() {
		return new JavaWrappedDStream<>(jssc, rdd.glom());
	}

	@Override
	public <R> JavaDStream<R> map(Function<T, R> arg0) {
		return new JavaWrappedDStream<>(jssc, rdd.map(arg0));
	}

	@Override
	public <U> JavaDStream<U> mapPartitions(FlatMapFunction<Iterator<T>, U> arg0) {
		return new JavaWrappedDStream<>(jssc, rdd.mapPartitions(arg0));
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<T>, K2, V2> arg0) {
		return new JavaWrappedPairDStream<>(jssc, rdd.mapPartitionsToPair(arg0));
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> mapToPair(PairFunction<T, K2, V2> arg0) {
		return new JavaWrappedPairDStream<>(jssc, rdd.mapToPair(arg0));
	}

	@Override
	public void print() {
		rdd.toDebugString();
	}

	@Override
	public void print(int arg0) {
		rdd.toDebugString();
	}

	@Override
	public JavaDStream<T> reduce(Function2<T, T, T> arg0) {
		return streaming(jssc, rdd.reduce(arg0));
	}

	@Override
	public DStream<T> reduceByWindow(scala.Function2<T, T, T> arg0, Duration arg1, Duration arg2) {
		throw new UnsupportedOperationException();
	}

	@Override
	public JavaDStream<T> reduceByWindow(Function2<T, T, T> arg0, Duration arg1, Duration arg2) {
		return streaming(jssc, rdd.reduce(arg0));
	}

	@Override
	public JavaDStream<T> reduceByWindow(Function2<T, T, T> arg0, Function2<T, T, T> arg1, Duration arg2, Duration arg3) {
		return streaming(jssc, rdd.reduce(arg0));
	}

	@Override
	public JavaDStream<Long> scalaIntToJavaLong(DStream<Object> arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<JavaRDD<T>> slice(Time arg0, Time arg1) {
		return Arrays.asList(rdd);
	}

	@Override
	public <U> JavaDStream<U> transform(Function<JavaRDD<T>, JavaRDD<U>> arg0) {
		try {
			return new JavaWrappedDStream<>(jssc, arg0.call(rdd));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <U> JavaDStream<U> transform(Function2<JavaRDD<T>, Time, JavaRDD<U>> arg0) {
		try {
			return new JavaWrappedDStream<>(jssc, arg0.call(rdd, new Time(new Date().getTime())));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> transformToPair(Function<JavaRDD<T>, JavaPairRDD<K2, V2>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2> JavaPairDStream<K2, V2> transformToPair(Function2<JavaRDD<T>, Time, JavaPairRDD<K2, V2>> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U, W> JavaDStream<W> transformWith(JavaDStream<U> arg0, Function3<JavaRDD<T>, JavaRDD<U>, Time, JavaRDD<W>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2, W> JavaDStream<W> transformWith(JavaPairDStream<K2, V2> arg0,
			Function3<JavaRDD<T>, JavaPairRDD<K2, V2>, Time, JavaRDD<W>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <U, K2, V2> JavaPairDStream<K2, V2> transformWithToPair(JavaDStream<U> arg0,
			Function3<JavaRDD<T>, JavaRDD<U>, Time, JavaPairRDD<K2, V2>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K2, V2, K3, V3> JavaPairDStream<K3, V3> transformWithToPair(JavaPairDStream<K2, V2> arg0,
			Function3<JavaRDD<T>, JavaPairRDD<K2, V2>, Time, JavaPairRDD<K3, V3>> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JavaRDD<T> wrapRDD(RDD<T> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@SafeVarargs
	static <R> JavaDStream<R> streaming(JavaStreamingContext jssc, R... r) {
		return new JavaWrappedDStream<R>(jssc, jssc.sparkContext().parallelize(Arrays.asList(r)));
	}

	static <K, V> JavaPairDStream<K, V> streaming(JavaStreamingContext jssc, Map<K, V> m) {
		List<Tuple2<K, V>> r = new ArrayList<>(m.size());
		for (Map.Entry<K, V> e : m.entrySet())
			r.add(new Tuple2<K, V>(e.getKey(), e.getValue()));
		return new JavaWrappedPairDStream<K, V>(jssc, jssc.sparkContext().parallelize(r).mapToPair(t -> t));
	}
}