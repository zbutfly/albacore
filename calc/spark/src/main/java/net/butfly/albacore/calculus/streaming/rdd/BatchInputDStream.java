package net.butfly.albacore.calculus.streaming.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Tuple2;

@Deprecated
class BatchInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -4894577198592085557L;
	private static final Logger logger = LoggerFactory.getLogger(BatchInputDStream.class);
	private Function2<Long, K[], JavaPairRDD<K, V>> batcher;
	private K[] offsets;
	private long limit;
	private long total = 0;

	public BatchInputDStream(JavaStreamingContext ssc, Function2<Long, K[], JavaPairRDD<K, V>> batcher, long batching, Class<K> kClass,
			Class<V> vClass) {
		super(ssc, kClass, vClass);
		this.batcher = batcher;
		this.limit = batching;
		this.offsets = null;
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time time) {
		try {
			current = batcher.call(offsets == null ? limit : limit + 1, offsets).cache();
		} catch (Exception e) {
			error(() -> "RDD batched failure", e);
			current = null;
		}
		if (null == current || current.isEmpty()) {
			info(() -> "RDD batched empty, batching finished.");
			jssc.stop(true, true);
		} else {
			// results exclude last item on prev batch
			if (null != offsets) {
				List<Tuple2<K, Object>> l = new ArrayList<>();
				for (K k : offsets)
					l.add(new Tuple2<K, Object>(k, null));
				current = current.subtractByKey(jssc.sparkContext().parallelize(l).mapToPair(t -> t));
			}
			if (null == current || current.isEmpty()) {
				info(() -> "RDD batched empty, batching finished.");
				jssc.stop(true, true);
			} else {
				// next offset is last item this time
				current.mapPartitionsWithIndex((i, it) -> {
					K last = null;
					while (it.hasNext())
						last = it.next()._1;
					if (logger.isTraceEnabled()) logger.trace("Batching on partition [" + i + "], found last key: " + last.toString());
					offsets[i] = last;
					return Arrays.asList(last).iterator();
				} , true);
			}
			if (logger.isTraceEnabled()) logger.trace("Total batched: " + (total += current.count()));
		}
		return super.compute(time);
	}
}