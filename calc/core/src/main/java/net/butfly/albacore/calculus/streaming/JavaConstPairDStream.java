package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ConstantInputDStream;

import scala.Tuple2;

public class JavaConstPairDStream<K, V> extends JavaPairInputDStream<K, V> implements WrappedPairRDDStream<K, V> {
	private static final long serialVersionUID = -983605896125973675L;

	public JavaConstPairDStream(JavaStreamingContext ssc, JavaPairRDD<K, V> rdd) {
		super(new ConstantInputDStream<Tuple2<K, V>>(ssc.ssc(), rdd.map(t -> t).rdd(), rdd.classTag()), rdd.kClassTag(), rdd.vClassTag());
	}

	@Override
	public long counts() {
		return this.compute(null).count();
	}
}
