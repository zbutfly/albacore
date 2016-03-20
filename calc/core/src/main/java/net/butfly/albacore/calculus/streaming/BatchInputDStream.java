package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Option;
import scala.Some;
import scala.Tuple2;

class BatchInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -4894577198592085557L;
	private Function2<Long, K, JavaPairRDD<K, V>> batcher;
	private K offset;
	private long limit;

	public BatchInputDStream(JavaStreamingContext ssc, Function2<Long, K, JavaPairRDD<K, V>> batcher, long batching, Class<K> kClass,
			Class<V> vClass) {
		super(ssc, kClass, vClass);
		this.batcher = batcher;
		this.limit = batching;
		this.offset = null;
	}

	@Override
	public void start() {}

	@Override
	public void stop() {}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
		try {
			current = batcher.call(offset == null ? limit : limit + 1, offset);
		} catch (Exception e) {
			logger.error("RDD batched failure", e);
			return super.compute(arg0);
		}
		// results exclude last item on prev batch
		if (null != offset) current = current.subtractByKey(jssc.sparkContext().parallelize(Arrays.asList(new Tuple2<K, Object>(offset,
				null))).mapToPair(t -> t));
		trace();
		if (current.isEmpty()) {
			logger.info("RDD batched empty, batching finished.");
			jssc.stop(true, true);
		}
		// next offset is last item this time
		offset = current.reduce((t1, t2) -> t1._1.toString().compareTo(t2._1.toString()) > 0 ? t1 : t2)._1;
		return new Some<RDD<Tuple2<K, V>>>(current.rdd());
	}
}