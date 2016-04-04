package net.butfly.albacore.calculus.streaming;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.ConstantInputDStream;
import org.apache.spark.streaming.dstream.DStream;

import com.google.common.base.Optional;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class JavaWrappedRDD<T> extends JavaRDD<T> implements JavaRDDLike<T, JavaRDD<T>> {
	private static final long serialVersionUID = 4496742087415223655L;
	JavaDStream<T> dstream;

	public JavaWrappedRDD(JavaDStream<T> dstream) {
		super(null, dstream.classTag());
		this.dstream = dstream;
	}

	public DStream<T> dstream() {
		return dstream.dstream();
	}

	@Override
	public RDD<T> rdd() {
		return javaRDD().rdd();
	}

	@Override
	public ClassTag<T> classTag() {
		return dstream.classTag();
	}

	public JavaRDD<T> javaRDD() {
		List<JavaRDD<T>> rdds = all();
		JavaRDD<T> rdd = rdds.get(0);
		for (int i = 1; i < rdds.size(); i++)
			rdd = rdd.union(rdds.get(i));
		return rdd;
	}

	@Override
	public JavaWrappedRDD<T> wrapRDD(RDD<T> rdd) {
		return new JavaWrappedRDD<>(
				JavaDStream.fromDStream(new ConstantInputDStream<>(dstream.context(), rdd, dstream.classTag()), dstream.classTag()));
	}

	@Override
	public <U> U aggregate(U zeroValue, Function2<U, T, U> seqOp, Function2<U, U, U> combOp) {
		return javaRDD().aggregate(zeroValue, seqOp, combOp);
	}

	@Override
	public <U> JavaPairRDD<T, U> cartesian(JavaRDDLike<U, ?> other) {
		return javaRDD().cartesian(other);
	}

	@Override
	public void checkpoint() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<T> collect() {
		return javaRDD().collect();
	}

	@Override
	public JavaFutureAction<List<T>> collectAsync() {
		return javaRDD().collectAsync();
	}

	@Override
	public List<T>[] collectPartitions(int[] partitionIds) {
		return javaRDD().collectPartitions(partitionIds);
	}

	@Override
	public SparkContext context() {
		return dstream.context().sparkContext();
	}

	@SuppressWarnings("deprecation")
	@Override
	public long count() {
		long[] r = new long[] { 0 };
		dstream.count().foreachRDD((Function<JavaRDD<Long>, Void>) rdd -> {
			r[0] += rdd.count();
			return null;
		});
		return r[0];
	}

	@Override
	public PartialResult<BoundedDouble> countApprox(long timeout) {
		return javaRDD().countApprox(timeout);
	}

	@Override
	public PartialResult<BoundedDouble> countApprox(long timeout, double confidence) {
		return javaRDD().countApprox(timeout, confidence);
	}

	@Override
	public long countApproxDistinct(double relativeSD) {
		return javaRDD().countApproxDistinct(relativeSD);
	}

	@Override
	public JavaFutureAction<Long> countAsync() {
		return javaRDD().countAsync();
	}

	@Override
	public Map<T, Long> countByValue() {
		return javaRDD().countByValue();
	}

	@Override
	public PartialResult<Map<T, BoundedDouble>> countByValueApprox(long timeout) {
		return javaRDD().countByValueApprox(timeout);
	}

	@Override
	public PartialResult<Map<T, BoundedDouble>> countByValueApprox(long timeout, double confidence) {
		return javaRDD().countByValueApprox(timeout, confidence);
	}

	@Override
	public T first() {
		return javaRDD().first();
	}

	@Override
	public <U> JavaRDD<U> flatMap(FlatMapFunction<T, U> func) {
		return javaRDD().flatMap(func);
	}

	@Override
	public JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunction<T> func) {
		return javaRDD().flatMapToDouble(func);
	}

	@Override
	public <K2, V2> JavaPairRDD<K2, V2> flatMapToPair(PairFlatMapFunction<T, K2, V2> func) {
		return javaRDD().flatMapToPair(func);
	}

	@Override
	public T fold(T arg0, Function2<T, T, T> arg1) {
		return javaRDD().fold(arg0, arg1);
	}

	@Override
	public void foreach(VoidFunction<T> arg0) {
		javaRDD().foreach(arg0);
	}

	@Override
	public JavaFutureAction<Void> foreachAsync(VoidFunction<T> arg0) {
		return javaRDD().foreachAsync(arg0);
	}

	@Override
	public void foreachPartition(VoidFunction<Iterator<T>> arg0) {
		javaRDD().foreachPartition(arg0);
	}

	@Override
	public JavaFutureAction<Void> foreachPartitionAsync(VoidFunction<Iterator<T>> arg0) {
		return javaRDD().foreachPartitionAsync(arg0);
	}

	@Override
	public Optional<String> getCheckpointFile() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getNumPartitions() {
		throw new UnsupportedOperationException();
	}

	@Override
	public StorageLevel getStorageLevel() {
		throw new UnsupportedOperationException();
	}

	@Override
	public JavaRDD<List<T>> glom() {
		return null;
	}

	@Override
	public <U> JavaPairRDD<U, Iterable<T>> groupBy(Function<T, U> arg0) {
		return javaRDD().groupBy(arg0);
	}

	@Override
	public <U> JavaPairRDD<U, Iterable<T>> groupBy(Function<T, U> arg0, int arg1) {
		return javaRDD().groupBy(arg0, arg1);
	}

	@Override
	public int id() {
		return javaRDD().id();
	}

	@Override
	public boolean isCheckpointed() {
		return false;
	}

	@Override
	public boolean isEmpty() {
		return javaRDD().isEmpty();
	}

	@Override
	public Iterator<T> iterator(Partition arg0, TaskContext arg1) {
		return javaRDD().iterator(arg0, arg1);
	}

	@Override
	public <U> JavaPairRDD<U, T> keyBy(Function<T, U> arg0) {
		return javaRDD().keyBy(arg0);
	}

	@Override
	public <R> JavaRDD<R> map(Function<T, R> arg0) {
		return new JavaWrappedRDD<>(dstream.map(arg0));
	}

	@Override
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>, U> arg0) {
		return new JavaWrappedRDD<>(dstream.mapPartitions(arg0));
	}

	@Override
	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>, U> arg0, boolean arg1) {
		return mapPartitions(arg0);
	}

	@Override
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<T>> arg0) {
		return javaRDD().mapPartitionsToDouble(arg0);
	}

	@Override
	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<T>> arg0, boolean arg1) {
		return javaRDD().mapPartitionsToDouble(arg0, arg1);
	}

	@Override
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<T>, K2, V2> arg0) {
		return new JavaWrappedRDD<Tuple2<K2, V2>>(dstream.mapPartitionsToPair(arg0).map(t -> t)).mapToPair(t -> t);
	}

	@Override
	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<T>, K2, V2> arg0, boolean arg1) {
		return mapPartitionsToPair(arg0);
	}

	@Override
	public <R> JavaRDD<R> mapPartitionsWithIndex(Function2<Integer, Iterator<T>, Iterator<R>> arg0, boolean arg1) {
		return javaRDD().mapPartitionsWithIndex(arg0, arg1);
	}

	@Override
	public <R> boolean mapPartitionsWithIndex$default$2() {
		return javaRDD().mapPartitionsWithIndex$default$2();
	}

	@Override
	public <R> JavaDoubleRDD mapToDouble(DoubleFunction<T> arg0) {
		return javaRDD().mapToDouble(arg0);
	}

	@Override
	public <K2, V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> arg0) {
		return new JavaWrappedRDD<Tuple2<K2, V2>>(dstream.mapToPair(arg0).map(t -> t)).mapToPair(t -> t);
	}

	@Override
	public T max(Comparator<T> arg0) {
		return javaRDD().max(arg0);
	}

	@Override
	public T min(Comparator<T> arg0) {
		return javaRDD().min(arg0);
	}

	@Override
	public String name() {
		return "Wrapped from DStream";
	}

	@Override
	public Optional<Partitioner> partitioner() {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("deprecation")
	@Override
	public List<Partition> partitions() {
		List<Partition> p = new ArrayList<>();
		dstream.foreachRDD(rdd -> {
			p.addAll(rdd.partitions());
			return null;
		});
		return p;
	}

	@Override
	public JavaRDD<String> pipe(String arg0) {
		return javaRDD().pipe(arg0);
	}

	@Override
	public JavaRDD<String> pipe(List<String> arg0) {
		return javaRDD().pipe(arg0);
	}

	@Override
	public JavaRDD<String> pipe(List<String> arg0, Map<String, String> arg1) {
		return javaRDD().pipe(arg0, arg1);
	}

	@Override
	public T reduce(Function2<T, T, T> arg0) {
		return javaRDD().reduce(arg0);
	}

	@Override
	public void saveAsObjectFile(String arg0) {
		javaRDD().saveAsObjectFile(arg0);
	}

	@Override
	public void saveAsTextFile(String arg0) {
		javaRDD().saveAsTextFile(arg0);
	}

	@Override
	public void saveAsTextFile(String arg0, Class<? extends CompressionCodec> arg1) {
		javaRDD().saveAsTextFile(arg0, arg1);
	}

	@Override
	@Deprecated
	public List<Partition> splits() {
		return partitions();
	}

	@Override
	public List<T> take(int arg0) {
		return javaRDD().take(arg0);
	}

	@Override
	public JavaFutureAction<List<T>> takeAsync(int arg0) {
		return javaRDD().takeAsync(arg0);
	}

	@Override
	public List<T> takeOrdered(int arg0) {
		return javaRDD().takeOrdered(arg0);
	}

	@Override
	public List<T> takeOrdered(int arg0, Comparator<T> arg1) {
		return javaRDD().takeOrdered(arg0, arg1);
	}

	@Override
	public List<T> takeSample(boolean arg0, int arg1) {
		return javaRDD().takeSample(arg0, arg1);
	}

	@Override
	public List<T> takeSample(boolean arg0, int arg1, long arg2) {
		return javaRDD().takeSample(arg0, arg1, arg2);
	}

	@Override
	@Deprecated
	public List<T> toArray() {
		return collect();
	}

	@Override
	public String toDebugString() {
		return "Wrapped from DStream";
	}

	@Override
	public Iterator<T> toLocalIterator() {
		return javaRDD().toLocalIterator();
	}

	@Override
	public List<T> top(int arg0) {
		return javaRDD().top(arg0);
	}

	@Override
	public List<T> top(int arg0, Comparator<T> arg1) {
		return javaRDD().top(arg0, arg1);
	}

	@Override
	public <U> U treeAggregate(U arg0, Function2<U, T, U> arg1, Function2<U, U, U> arg2) {
		return javaRDD().treeAggregate(arg0, arg1, arg2);
	}

	@Override
	public <U> U treeAggregate(U arg0, Function2<U, T, U> arg1, Function2<U, U, U> arg2, int arg3) {
		return javaRDD().treeAggregate(arg0, arg1, arg2, arg3);
	}

	@Override
	public T treeReduce(Function2<T, T, T> arg0) {
		return javaRDD().treeReduce(arg0);
	}

	@Override
	public T treeReduce(Function2<T, T, T> arg0, int arg1) {
		return javaRDD().treeReduce(arg0, arg1);
	}

	@Override
	public <U> JavaPairRDD<T, U> zip(JavaRDDLike<U, ?> arg0) {
		return javaRDD().zip(arg0);
	}

	@Override
	public JavaPairRDD<T, Long> zipWithIndex() {
		return javaRDD().zipWithIndex();
	}

	@Override
	public JavaPairRDD<T, Long> zipWithUniqueId() {
		return javaRDD().zipWithUniqueId();
	}

	@Override
	public <U, VV> JavaRDD<VV> zipPartitions(JavaRDDLike<U, ?> arg0, FlatMapFunction2<Iterator<T>, Iterator<U>, VV> arg1) {
		return javaRDD().zipPartitions(arg0, arg1);
	}

	@SuppressWarnings("deprecation")
	private List<JavaRDD<T>> all() {
		List<JavaRDD<T>> rdds = new ArrayList<>();
		this.dstream.foreachRDD((Function<JavaRDD<T>, Void>) r -> {
			rdds.add(r);
			return null;
		});
		return rdds;
	}

	protected JavaRDD<T> empty() {
		return JavaRDD.fromRDD(dstream.context().sparkContext().emptyRDD(this.classTag()), dstream.classTag());
	}
}
