package net.butfly.albacore.calculus.streaming;

import java.util.Comparator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;

import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

@Deprecated
class RDDBatchInputDStream<K, V> extends RDDDStream<Tuple2<K, V>> {
	private StreamingContext ssc;
	private long limit = Long.MAX_VALUE;
	private K offset;
	private Function2<Long, K, RDD<Tuple2<K, V>>> batcher;
	private Comparator<K> comparator;

	public RDDBatchInputDStream(StreamingContext ssc, long batch, Function2<Long, K, RDD<Tuple2<K, V>>> batcher, Comparator<K> comparator,
			ClassTag<Tuple2<K, V>> classTag) {
		super(ssc, classTag);
		this.ssc = ssc;
		this.batcher = batcher;
		this.comparator = comparator;
		load();
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time time) {
		load();
		// results exclude last item on prev batch
		if (null != offset) current = JavaRDD.fromRDD(current, classTag)
				.subtract(RDDDStream.rdd(new JavaSparkContext(sc), new Tuple2<K, V>(offset, null))).rdd();
		if (current.isEmpty()) ssc.stop(true, true);
		// next offset is last item this time
		else offset = JavaRDD.fromRDD(current, classTag).treeReduce((t1, t2) -> comparator.compare(t1._1, t2._1) < 0 ? t1 : t2)._1;
		return super.compute(time);
	}

	@Override
	protected RDD<Tuple2<K, V>> load() {
		try {
			current = batcher.call(offset == null ? limit : limit + 1, offset).cache();
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
		if (current == null) throw new IllegalArgumentException("RDD null illegal in streamizing");
		return current;
	}
}