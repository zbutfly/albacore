package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ConstantInputDStream;

import scala.Tuple2;

public class JavaConstPairDStream<K, V> extends JavaWrappedPairInputDStream<K, V, ConstantInputDStream<Tuple2<K, V>>> {
	private static final long serialVersionUID = -983605896125973675L;

	@SuppressWarnings("unchecked")
	public JavaConstPairDStream(JavaStreamingContext ssc, JavaPairRDD<K, V> rdd) {
		super(new ConstantInputDStream<Tuple2<K, V>>(ssc.ssc(), rdd.map(t -> t).rdd(), rdd.classTag()),
				(Class<K>) rdd.kClassTag().runtimeClass(), (Class<V>) rdd.vClassTag().runtimeClass());
	}
}
