package net.butfly.albacore.calculus.factor;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;

public class PairRDS<K, F extends Factor<F>> {
	JavaPairRDD<K, F> rdd;
	JavaPairDStream<K, F> dstream;
}
