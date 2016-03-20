package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Some;
import scala.Tuple2;

class BatchInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -4894577198592085557L;
	private static Logger logger = LoggerFactory.getLogger(JavaBatchPairDStream.class);
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
			JavaPairRDD<K, V> r = batcher.call(offset == null ? limit : limit + 1, offset);
			if (r.isEmpty()) {
				logger.info("RDD batched empty, batching finished.");
				this.ssc().stop(true, true);
			}
			// results exclude last item on prev batch
			current = null == offset ? r
					: r.subtractByKey(
							jssc.sparkContext().parallelize(Arrays.asList(new Tuple2<K, Object>(offset, null))).mapToPair(t -> t));
			offset = r.reduce((t1, t2) -> t1._1.toString().compareTo(t2._1.toString()) > 0 ? t1 : t2)._1;
			if (logger.isTraceEnabled())
				logger.trace("RDD batched from data source on streaming computing, " + current.count() + " fetched.");
			return new Some<RDD<Tuple2<K, V>>>(current.rdd());
		} catch (Exception e) {
			logger.error("Failure refresh dstream from rdd", e);
			return super.compute(arg0);
		}
	}
}