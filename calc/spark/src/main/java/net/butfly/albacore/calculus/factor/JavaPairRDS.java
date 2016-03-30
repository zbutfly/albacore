//package net.butfly.albacore.calculus.factor;
//
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.spark.Partition;
//import org.apache.spark.Partitioner;
//import org.apache.spark.SparkContext;
//import org.apache.spark.TaskContext;
//import org.apache.spark.api.java.JavaDoubleRDD;
//import org.apache.spark.api.java.JavaFutureAction;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaRDDLike;
//import org.apache.spark.api.java.function.DoubleFlatMapFunction;
//import org.apache.spark.api.java.function.DoubleFunction;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.FlatMapFunction2;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.Function3;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.api.java.function.VoidFunction2;
//import org.apache.spark.partial.BoundedDouble;
//import org.apache.spark.partial.PartialResult;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.StreamingContext;
//import org.apache.spark.streaming.Time;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaDStreamLike;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.dstream.DStream;
//
//import com.google.common.base.Optional;
//
//import scala.Tuple2;
//import scala.reflect.ClassTag;
//
//public abstract class JavaPairRDS<K, F extends Factor<F>>
//		implements JavaRDDLike<Tuple2<K, F>, JavaPairRDS<K, F>>, JavaDStreamLike<Tuple2<K, F>, JavaPairRDS<K, F>, JavaPairRDS<K, F>> {
//	private static final long serialVersionUID = -147508356270887375L;
//	JavaPairRDD<K, F> rdd;
//	JavaPairDStream<K, F> dstream;
//
//	@Override
//	public DStream<Tuple2<K, F>> checkpoint(Duration arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairDStream<Tuple2<K, F>, Long> countByValue(int arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairDStream<Tuple2<K, F>, Long> countByValueAndWindow(Duration arg0, Duration arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairDStream<Tuple2<K, F>, Long> countByValueAndWindow(Duration arg0, Duration arg1, int arg2) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<Long> countByWindow(Duration arg0, Duration arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public DStream<Tuple2<K, F>> dstream() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void foreach(Function<JavaPairRDS<K, F>, Void> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void foreach(Function2<JavaPairRDS<K, F>, Time, Void> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void foreachRDD(Function<JavaPairRDS<K, F>, Void> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void foreachRDD(Function2<JavaPairRDS<K, F>, Time, Void> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void foreachRDD(VoidFunction<JavaPairRDS<K, F>> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void foreachRDD(VoidFunction2<JavaPairRDS<K, F>, Time> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void print() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void print(int arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public DStream<Tuple2<K, F>> reduceByWindow(scala.Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg0, Duration arg1,
//			Duration arg2) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<Tuple2<K, F>> reduceByWindow(Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg0, Duration arg1,
//			Duration arg2) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<Tuple2<K, F>> reduceByWindow(Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg0,
//			Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg1, Duration arg2, Duration arg3) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<Long> scalaIntToJavaLong(DStream<Object> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<JavaPairRDS<K, F>> slice(Time arg0, Time arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaDStream<U> transform(Function<JavaPairRDS<K, F>, JavaRDD<U>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaDStream<U> transform(Function2<JavaPairRDS<K, F>, Time, JavaRDD<U>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2> JavaPairDStream<K2, V2> transformToPair(Function<JavaPairRDS<K, F>, JavaPairRDD<K2, V2>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2> JavaPairDStream<K2, V2> transformToPair(Function2<JavaPairRDS<K, F>, Time, JavaPairRDD<K2, V2>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U, W> JavaDStream<W> transformWith(JavaDStream<U> arg0, Function3<JavaPairRDS<K, F>, JavaRDD<U>, Time, JavaRDD<W>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2, W> JavaDStream<W> transformWith(JavaPairDStream<K2, V2> arg0,
//			Function3<JavaPairRDS<K, F>, JavaPairRDD<K2, V2>, Time, JavaRDD<W>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U, K2, V2> JavaPairDStream<K2, V2> transformWithToPair(JavaDStream<U> arg0,
//			Function3<JavaPairRDS<K, F>, JavaRDD<U>, Time, JavaPairRDD<K2, V2>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2, K3, V3> JavaPairDStream<K3, V3> transformWithToPair(JavaPairDStream<K2, V2> arg0,
//			Function3<JavaPairRDS<K, F>, JavaPairRDD<K2, V2>, Time, JavaPairRDD<K3, V3>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> U aggregate(U arg0, Function2<U, Tuple2<K, F>, U> arg1, Function2<U, U, U> arg2) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaPairRDD<Tuple2<K, F>, U> cartesian(JavaRDDLike<U, ?> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void checkpoint() {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public ClassTag<Tuple2<K, F>> classTag() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> collect() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaFutureAction<List<Tuple2<K, F>>> collectAsync() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>>[] collectPartitions(int[] arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public PartialResult<BoundedDouble> countApprox(long arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public PartialResult<BoundedDouble> countApprox(long arg0, double arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public long countApproxDistinct(double arg0) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public JavaFutureAction<Long> countAsync() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public PartialResult<Map<Tuple2<K, F>, BoundedDouble>> countByValueApprox(long arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public PartialResult<Map<Tuple2<K, F>, BoundedDouble>> countByValueApprox(long arg0, double arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Tuple2<K, F> first() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunction<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Tuple2<K, F> fold(Tuple2<K, F> arg0, Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void foreach(VoidFunction<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public JavaFutureAction<Void> foreachAsync(VoidFunction<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void foreachPartition(VoidFunction<Iterator<Tuple2<K, F>>> arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public JavaFutureAction<Void> foreachPartitionAsync(VoidFunction<Iterator<Tuple2<K, F>>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Optional<String> getCheckpointFile() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public int getNumPartitions() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public StorageLevel getStorageLevel() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaPairRDD<U, Iterable<Tuple2<K, F>>> groupBy(Function<Tuple2<K, F>, U> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaPairRDD<U, Iterable<Tuple2<K, F>>> groupBy(Function<Tuple2<K, F>, U> arg0, int arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public int id() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public boolean isCheckpointed() {
//		// TODO Auto-generated method stub
//		return false;
//	}
//
//	@Override
//	public boolean isEmpty() {
//		// TODO Auto-generated method stub
//		return false;
//	}
//
//	@Override
//	public Iterator<Tuple2<K, F>> iterator(Partition arg0, TaskContext arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaPairRDD<U, Tuple2<K, F>> keyBy(Function<Tuple2<K, F>, U> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<Tuple2<K, F>>, U> arg0, boolean arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<Tuple2<K, F>>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<Iterator<Tuple2<K, F>>> arg0, boolean arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2> JavaPairRDD<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Tuple2<K, F>>, K2, V2> arg0, boolean arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <R> JavaRDD<R> mapPartitionsWithIndex(Function2<Integer, Iterator<Tuple2<K, F>>, Iterator<R>> arg0, boolean arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <R> boolean mapPartitionsWithIndex$default$2() {
//		// TODO Auto-generated method stub
//		return false;
//	}
//
//	@Override
//	public <R> JavaDoubleRDD mapToDouble(DoubleFunction<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Tuple2<K, F> max(Comparator<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Tuple2<K, F> min(Comparator<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public String name() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Optional<Partitioner> partitioner() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Partition> partitions() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaRDD<String> pipe(String arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaRDD<String> pipe(List<String> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaRDD<String> pipe(List<String> arg0, Map<String, String> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public RDD<Tuple2<K, F>> rdd() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public void saveAsObjectFile(String arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void saveAsTextFile(String arg0) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public void saveAsTextFile(String arg0, Class<? extends CompressionCodec> arg1) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public List<Partition> splits() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> take(int arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaFutureAction<List<Tuple2<K, F>>> takeAsync(int arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> takeOrdered(int arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> takeOrdered(int arg0, Comparator<Tuple2<K, F>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> takeSample(boolean arg0, int arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> takeSample(boolean arg0, int arg1, long arg2) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> toArray() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public String toDebugString() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Iterator<Tuple2<K, F>> toLocalIterator() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> top(int arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Tuple2<K, F>> top(int arg0, Comparator<Tuple2<K, F>> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> U treeAggregate(U arg0, Function2<U, Tuple2<K, F>, U> arg1, Function2<U, U, U> arg2) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> U treeAggregate(U arg0, Function2<U, Tuple2<K, F>, U> arg1, Function2<U, U, U> arg2, int arg3) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Tuple2<K, F> treeReduce(Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public Tuple2<K, F> treeReduce(Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg0, int arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairRDS<K, F> wrapRDD(RDD<Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaPairRDD<Tuple2<K, F>, U> zip(JavaRDDLike<U, ?> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U, V> JavaRDD<V> zipPartitions(JavaRDDLike<U, ?> arg0, FlatMapFunction2<Iterator<Tuple2<K, F>>, Iterator<U>, V> arg1) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairRDD<Tuple2<K, F>, Long> zipWithIndex() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairRDD<Tuple2<K, F>, Long> zipWithUniqueId() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//	@Override
//	public SparkContext context() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<Long> count() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaPairDStream<Tuple2<K, F>, Long> countByValue() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaDStream<U> flatMap(FlatMapFunction<Tuple2<K, F>, U> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2> JavaPairDStream<K2, V2> flatMapToPair(PairFlatMapFunction<Tuple2<K, F>, K2, V2> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<List<Tuple2<K, F>>> glom() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <R> JavaDStream<R> map(Function<Tuple2<K, F>, R> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <U> JavaDStream<U> mapPartitions(FlatMapFunction<Iterator<Tuple2<K, F>>, U> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2> JavaPairDStream<K2, V2> mapPartitionsToPair(PairFlatMapFunction<Iterator<Tuple2<K, F>>, K2, V2> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K2, V2> JavaPairDStream<K2, V2> mapToPair(PairFunction<Tuple2<K, F>, K2, V2> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public JavaDStream<Tuple2<K, F>> reduce(Function2<Tuple2<K, F>, Tuple2<K, F>, Tuple2<K, F>> arg0) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//}
