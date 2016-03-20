package net.butfly.albacore.calculus.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;

@SuppressWarnings("unchecked")
public class JavaBatchPairDStream<K, V> extends JavaPairInputDStream<K, V> implements WrappedPairRDDStream<K, V> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaBatchPairDStream(JavaStreamingContext ssc, Function2<Long, K, JavaPairRDD<K, V>> batcher, long batching, Class<K> kClass,
			Class<V> vClass) {
		super(new BatchInputDStream<>(ssc.ssc(), batcher, batching, ManifestFactory.classType(kClass), ManifestFactory.classType(vClass)),
				ManifestFactory.classType(kClass), ManifestFactory.classType(vClass));
		((BatchInputDStream<K, V>) this.dstream()).jssc = ssc;
	}

	@Override
	public long counts() {
		return ((BatchInputDStream<K, V>) this.dstream()).current.count();
	}

	private static class BatchInputDStream<K, V> extends InputDStream<Tuple2<K, V>> {
		private static Logger logger = LoggerFactory.getLogger(JavaBatchPairDStream.class);
		public JavaStreamingContext jssc;
		private Function2<Long, K, JavaPairRDD<K, V>> batcher;
		private JavaPairRDD<K, V> current;
		private K offset;
		private long limit;

		public BatchInputDStream(StreamingContext ssc, Function2<Long, K, JavaPairRDD<K, V>> batcher, long batching, ClassTag<K> kClass,
				ClassTag<V> vClass) {
			super(ssc, ManifestFactory.classType((Class<?>) new TypeToken<Tuple2<K, V>>() {
				private static final long serialVersionUID = 2239093404972667748L;
			}.getType()));
			this.batcher = batcher;
			this.limit = batching;
			this.offset = null;
			JavaRDD<Tuple2<K, V>> e = jssc.sparkContext().emptyRDD();
			this.current = e.mapToPair(t -> t);
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
				return new Some<RDD<Tuple2<K, V>>>(super.ssc().sc().emptyRDD(super.org$apache$spark$streaming$dstream$DStream$$evidence$1));
			}
		}
	}
}
