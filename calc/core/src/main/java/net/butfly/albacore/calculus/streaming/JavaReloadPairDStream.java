package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
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

public class JavaReloadPairDStream<K, V> extends JavaPairInputDStream<K, V> implements WrappedPairRDDStream<K, V> {
	private static final long serialVersionUID = -7741510780623981966L;

	public JavaReloadPairDStream(JavaStreamingContext ssc, Function0<JavaPairRDD<K, V>> loader, Class<K> kClass, Class<V> vClass) {
		super(new RefreshableInputDStream<K, V>(ssc.ssc(), () -> loader.call().rdd(),
				ManifestFactory.classType((Class<?>) new TypeToken<Tuple2<K, V>>() {
					private static final long serialVersionUID = 2239093404972667748L;
				}.getType())), ManifestFactory.classType(kClass), ManifestFactory.classType(vClass));
	}

	@SuppressWarnings("unchecked")
	@Override
	public long counts() {
		return ((RefreshableInputDStream<K, V>) this.dstream()).current.count();
	}

	private static class RefreshableInputDStream<K, V> extends InputDStream<Tuple2<K, V>> {
		private static Logger logger = LoggerFactory.getLogger(RefreshableInputDStream.class);
		private Function0<RDD<Tuple2<K, V>>> loader;
		private RDD<Tuple2<K, V>> current;

		public RefreshableInputDStream(StreamingContext ssc, Function0<RDD<Tuple2<K, V>>> loader, ClassTag<Tuple2<K, V>> classTag) {
			super(ssc, classTag);
			this.loader = loader;
			this.current = this.ssc().sc().emptyRDD(classTag);
		}

		@Override
		public void start() {}

		@Override
		public void stop() {}

		@Override
		public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
			try {
				this.current = loader.call();
				if (logger.isTraceEnabled())
					logger.trace("RDD reloaded from data source on streaming computing, " + current.count() + " fetched.");
				else if (logger.isDebugEnabled()) logger.debug("RDD reloaded from data source on streaming computing.");
				return new Some<RDD<Tuple2<K, V>>>(current);
			} catch (Exception e) {
				logger.error("Failure refresh dstream from rdd", e);
				return new Some<RDD<Tuple2<K, V>>>(super.ssc().sc().emptyRDD(ManifestFactory.classType(Tuple2.class)));
			}
		}
	}
}
