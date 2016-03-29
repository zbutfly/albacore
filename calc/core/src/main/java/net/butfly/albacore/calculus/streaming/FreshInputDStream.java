package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Tuple2;

class FreshInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -7855142157425876073L;
	private static Logger logger = LoggerFactory.getLogger(JavaFreshPairDStream.class);
	private Function0<JavaPairRDD<K, V>> loader;

	public FreshInputDStream(JavaStreamingContext ssc, Function0<JavaPairRDD<K, V>> loader, Class<K> kClass, Class<V> vClass) {
		super(ssc, kClass, vClass);
		this.loader = loader;
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
		try {
			current = loader.call();
		} catch (Exception e) {
			logger.error("RDD reloaded failure", e);
			current = null;
		}
		return super.compute(arg0);
	}
}