package net.butfly.albacore.calculus.streaming.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Option;
import scala.Tuple2;

class StockInputDStream<K, V> extends WrappedPairInputDStream<K, V> {
	private static final long serialVersionUID = -7855142157425876073L;

	public StockInputDStream(JavaStreamingContext ssc, JavaPairRDD<K, V> rdd, Class<K> kClass, Class<V> vClass) {
		super(ssc, kClass, vClass);
		this.current = rdd;
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
		try {
			return super.compute(arg0);
		} finally {
			current = null;
		}
	}
}