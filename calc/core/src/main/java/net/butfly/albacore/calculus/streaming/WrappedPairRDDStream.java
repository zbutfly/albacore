package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

public interface WrappedPairRDDStream<K, V> extends JavaDStreamLike<Tuple2<K, V>, JavaPairDStream<K, V>, JavaPairRDD<K, V>> {
	@SuppressWarnings("deprecation")
	default long counts() {
		long[] r = new long[] { 0 };
		this.count().foreachRDD((Function<JavaRDD<Long>, Void>) v1 -> {
			r[0] += v1.count();
			return null;
		});
		return r[0];
	}
}
