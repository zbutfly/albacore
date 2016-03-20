package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.reflect.ClassTag;

public abstract class WrappedPairInputDStream<K, V> extends InputDStream<Tuple2<K, V>> {
	public JavaStreamingContext jssc;
	protected JavaPairRDD<K, V> current;

	public WrappedPairInputDStream(JavaStreamingContext ssc, Class<K> kClass, Class<V> vClass) {
		super(ssc.ssc(), classTag(ssc.sparkContext(), kClass, vClass));
		this.jssc = ssc;
		this.current = emptyPairRDD();
	}

	private JavaPairRDD<K, V> emptyPairRDD() {
		return emptyRDD().mapToPair(t -> t);
	}

	private JavaRDD<Tuple2<K, V>> emptyRDD() {
		return jssc.sparkContext().emptyRDD();
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
		return new Some<RDD<Tuple2<K, V>>>(emptyRDD().rdd());
	}

	private static <K, V> ClassTag<Tuple2<K, V>> classTag(JavaSparkContext sc, Class<K> kClass, Class<V> vClass) {
		JavaRDD<Tuple2<K, V>> r = sc.emptyRDD();
		return r.classTag();
	}
}