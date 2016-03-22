package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Option;
import scala.Tuple2;

class BatchInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -4894577198592085557L;
	private Function2<Long, K, JavaPairRDD<K, V>> batcher;
	private K offset;
	private long limit;
	private long total = 0;

	public BatchInputDStream(JavaStreamingContext ssc, Function2<Long, K, JavaPairRDD<K, V>> batcher, long batching, Class<K> kClass,
			Class<V> vClass) {
		super(ssc, kClass, vClass);
		this.batcher = batcher;
		this.limit = batching;
		this.offset = null;
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
		try {
			current = batcher.call(offset == null ? limit : limit + 1, offset);
		} catch (Exception e) {
			error(() -> "RDD batched failure", e);
			return super.compute(arg0);
		}
		if (null == current || current.isEmpty()) {
			info(() -> "RDD batched empty, batching finished.");
			jssc.stop(true, true);
			return Option.empty();
		} else {
			// results exclude last item on prev batch
			if (null != offset) current = current
					.subtractByKey(jssc.sparkContext().parallelize(Arrays.asList(new Tuple2<K, Object>(offset, null))).mapToPair(t -> t));
			if (null == current || current.isEmpty()) {
				info(() -> "RDD batched empty, batching finished.");
				jssc.stop(true, true);
				return Option.empty();
			}
			K last = current.reduce((t1, t2) -> t1._1.toString().compareTo(t2._1.toString()) > 0 ? t1 : t2)._1;
			trace(() -> {
				long c = current.count();
				total += c;
				return "RDD [" + name() + "] computed: " + c + ", key from [" + (null == offset ? "null" : offset.toString())
						+ "](excluded) to [" + last.toString() + "](include), total traced: " + total + ".";
			});
			// next offset is last item this time
			offset = last;
			return Option.apply(current.rdd());
		}
	}
}