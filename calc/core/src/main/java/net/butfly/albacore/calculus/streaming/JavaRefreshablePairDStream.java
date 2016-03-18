package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.utils.Reflections;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;

public class JavaRefreshablePairDStream<K, V> extends JavaPairInputDStream<K, V> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaRefreshablePairDStream(JavaStreamingContext ssc, Function0<JavaPairRDD<K, V>> func) {
		super(new RefreshableInputDStream<Tuple2<K, V>>(ssc.ssc(), () -> func.call().rdd(), ManifestFactory.classType(Tuple2.class)),
				ManifestFactory.classType(Reflections.resolveGenericParameter(
						Reflections.resolveGenericParameter(func.getClass(), Function.class, "R"), JavaPairRDD.class, "K")),
				ManifestFactory.classType(Reflections.resolveGenericParameter(
						Reflections.resolveGenericParameter(func.getClass(), Function.class, "R"), JavaPairRDD.class, "V")));
	}

	private static class RefreshableInputDStream<T> extends InputDStream<T> {
		private Function0<RDD<T>> func;
		private Logger logger = LoggerFactory.getLogger(RefreshableInputDStream.class);

		public RefreshableInputDStream(StreamingContext ssc, Function0<RDD<T>> func, ClassTag<T> classTag) {
			super(ssc, classTag);
			this.func = func;
		}

		@Override
		public void start() {}

		@Override
		public void stop() {}

		@Override
		public Option<RDD<T>> compute(Time arg0) {
			try {
				RDD<T> r = func.call();
				if (logger.isTraceEnabled())
					logger.trace("RDD refetched from data source on streaming computing, " + r.count() + " fetched.");
				else if (logger.isDebugEnabled()) logger.debug("RDD refetched from data source on streaming computing.");
				return new Some<RDD<T>>(r);
			} catch (Exception e) {
				logger.error("Failure refresh dstream from rdd", e);
				return new Some<RDD<T>>(super.ssc().sc().emptyRDD(ManifestFactory.classType(Reflections.resolveGenericParameter(
						Reflections.resolveGenericParameter(func.getClass(), Function.class, "R"), RDD.class, "T"))));
			}
		}
	}
}
