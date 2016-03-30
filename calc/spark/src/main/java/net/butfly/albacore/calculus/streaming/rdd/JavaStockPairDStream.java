package net.butfly.albacore.calculus.streaming.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Option;
import scala.Tuple2;

public class JavaStockPairDStream<K, V> extends JavaWrappedPairInputDStream<K, V, StockInputDStream<K, V>> {
	private static final long serialVersionUID = -983605896125973675L;

	@SuppressWarnings("unchecked")
	public JavaStockPairDStream(JavaStreamingContext ssc, JavaPairRDD<K, V> rdd) {
		super(new StockInputDStream<K, V>(ssc, rdd, (Class<K>) rdd.kClassTag().runtimeClass(), (Class<V>) rdd.vClassTag().runtimeClass()),
				(Class<K>) rdd.kClassTag().runtimeClass(), (Class<V>) rdd.vClassTag().runtimeClass());
	}

	public long counts() {
		Option<RDD<Tuple2<K, V>>> o = this.dstream().compute(null);
		return o.isDefined() && !o.isEmpty() ? o.get().count() : 0;
	}

	public void cleanup() {
		Option<RDD<Tuple2<K, V>>> o = this.dstream().compute(null);
		if (o.isDefined()) o.get().clearDependencies();
	}
}
