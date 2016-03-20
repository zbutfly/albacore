package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Some;
import scala.Tuple2;

class RefreshableInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -7855142157425876073L;
	private static Logger logger = LoggerFactory.getLogger(JavaReloadPairDStream.class);
	private Function0<JavaPairRDD<K, V>> loader;

	public RefreshableInputDStream(JavaStreamingContext ssc, Function0<JavaPairRDD<K, V>> loader, Class<K> kClass, Class<V> vClass) {
		super(ssc, kClass, vClass);
		this.loader = loader;
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
			return new Some<RDD<Tuple2<K, V>>>(current.rdd());
		} catch (Exception e) {
			logger.error("Failure refresh dstream from rdd", e);
			jssc.sparkContext().emptyRDD();
			return super.compute(arg0);
		}
	}
}