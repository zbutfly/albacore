package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

/**
 * unify rdd / dstream
 * 
 * @author zx
 *
 */
public class JavaPairRDS<K, V> implements Serializable {
	private static final long serialVersionUID = -4684343907651750780L;

	private enum Type {
		RDD, DSTREAM
	}

	private Type type;
	private JavaPairRDD<K, V> rdd;
	private JavaPairDStream<K, V> dstream;

	public JavaPairRDS(JavaPairRDD<K, V> rdd) {
		this.type = Type.RDD;
		this.rdd = rdd;
		this.dstream = null;
	}

	public JavaPairRDS(JavaPairDStream<K, V> dstream) {
		this.type = Type.DSTREAM;
		this.rdd = null;
		this.dstream = dstream;
	}

	public <K2, V2> JavaPairRDS<K2, V2> mapToPair(PairFunction<Tuple2<K, V>, K2, V2> f) {
		switch (type) {
		case RDD:
			return new JavaPairRDS<K2, V2>(rdd.mapToPair(f));
		case DSTREAM:
			return new JavaPairRDS<K2, V2>(dstream.mapToPair(f));
		default:
			throw new UnsupportedOperationException();
		}
	}

	public <W> JavaPairRDS<K, Tuple2<V, W>> join(JavaPairRDS<K, W> other) {
		switch (type) {
		case RDD:
			switch (other.type) {
			case RDD:
				return new JavaPairRDS<K, Tuple2<V, W>>(rdd.join(other.rdd));
			case DSTREAM:
				Queue<JavaPairRDD<K, V>> q = new ArrayBlockingQueue<JavaPairRDD<K, V>>(1);
				q.add(this.rdd);
				// other.dstream.context().qqueueStream(q);
				// return new JavaPairRDS<K, Tuple2<V,
				// W>>(other.dstream.join(.qu)dstream.join(other.dstream));
			default:
				throw new UnsupportedOperationException();
			}
		case DSTREAM:
			return new JavaPairRDS<K, Tuple2<V, W>>(dstream.join(other.dstream));
		default:
			throw new UnsupportedOperationException();
		}
	}
}
