package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
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

import net.butfly.albacore.calculus.utils.Reflections;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;

public class JavaBatchPairDStream<K, V> extends JavaPairInputDStream<K, V> implements WrappedPairRDDStream<K, V> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaBatchPairDStream(JavaStreamingContext ssc, Function2<Integer, Integer, JavaPairRDD<K, V>> batcher, int batching,
			Class<K> kClass, Class<V> vClass) {
		super(new BatchInputDStream<K, V>(ssc.ssc(), (limit, offset) -> batcher.call(limit, offset).rdd(), batching,
				ManifestFactory.classType((Class<?>) new TypeToken<Tuple2<K, V>>() {
					private static final long serialVersionUID = 2239093404972667748L;
				}.getType())), ManifestFactory.classType(kClass), ManifestFactory.classType(vClass));
	}

	@SuppressWarnings("unchecked")
	@Override
	public long counts() {
		return ((BatchInputDStream<K, V>) this.dstream()).current.count();
	}

	private static class BatchInputDStream<K, V> extends InputDStream<Tuple2<K, V>> {
		private static Logger logger = LoggerFactory.getLogger(BatchInputDStream.class);
		private Function2<Integer, Integer, RDD<Tuple2<K, V>>> batcher;
		private RDD<Tuple2<K, V>> current;
		private int offset;
		private int limit;

		public BatchInputDStream(StreamingContext ssc, Function2<Integer, Integer, RDD<Tuple2<K, V>>> batcher, int batching,
				ClassTag<Tuple2<K, V>> classTag) {
			super(ssc, classTag);
			this.batcher = batcher;
			this.limit = batching;
			this.offset = 0;
			this.current = this.ssc().sc().emptyRDD(classTag);
		}

		@Override
		public void start() {}

		@Override
		public void stop() {}

		@Override
		public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
			try {
				RDD<Tuple2<K, V>> r = batcher.call(limit, offset);
				if (r.isEmpty()) {
					logger.info("RDD batched empty, batching finished.");
					this.ssc().stop(true, true);
				}
				current = r;
				long exactly = r.count();
				offset += exactly;
				if (logger.isDebugEnabled()) logger.debug("RDD batched from data source on streaming computing, " + exactly + " fetched.");
				return new Some<RDD<Tuple2<K, V>>>(r);
			} catch (Exception e) {
				logger.error("Failure refresh dstream from rdd", e);
				return new Some<RDD<Tuple2<K, V>>>(super.ssc().sc().emptyRDD(ManifestFactory.classType(Reflections.resolveGenericParameter(
						Reflections.resolveGenericParameter(batcher.getClass(), Function.class, "R"), RDD.class, "T"))));
			}
		}
	}
}
