package net.butfly.albacore.calculus.streaming;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.InputDStream;

import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction0;

public abstract class WrappedPairInputDStream<K, V> extends InputDStream<Tuple2<K, V>> {
	public JavaStreamingContext jssc;
	protected JavaPairRDD<K, V> current;

	public WrappedPairInputDStream(JavaStreamingContext ssc, Class<K> kClass, Class<V> vClass) {
		super(ssc.ssc(), classTag(ssc.sparkContext(), kClass, vClass));
		this.jssc = ssc;
		this.current = null;
	}

	@Override
	public Option<RDD<Tuple2<K, V>>> compute(Time arg0) {
		return Option.empty();
	}

	private static <K, V> ClassTag<Tuple2<K, V>> classTag(JavaSparkContext sc, Class<K> kClass, Class<V> vClass) {
		JavaRDD<Tuple2<K, V>> r = sc.emptyRDD();
		return r.classTag();
	}

	@Override
	public String name() {
		return super.name() + "[for " + this.org$apache$spark$streaming$dstream$DStream$$evidence$1.toString() + "]";
	}

	protected void trace(Function0<String> msg) {
		this.logTrace(new AbstractFunction0<String>() {
			@Override
			public String apply() {
				try {
					return msg.call();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	protected void info(Function0<String> msg) {
		this.logInfo(new AbstractFunction0<String>() {
			@Override
			public String apply() {
				try {
					return msg.call();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	protected void error(Function0<String> msg, Throwable th) {
		this.logError(new AbstractFunction0<String>() {
			@Override
			public String apply() {
				try {
					return msg.call();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}, th);
	}

	@Override
	public void start() {}

	@Override
	public void stop() {}
}