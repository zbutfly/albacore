package net.butfly.albacore.calculus;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.factor.Factors;
import net.butfly.albacore.calculus.functor.Functor;

public abstract class Calculus<OUTK, OUTV extends Factor<OUTV>> implements Serializable {
	private static final long serialVersionUID = 6432707546470042520L;
	protected final Logger logger;

	public Calculus() {
		this.logger = LoggerFactory.getLogger(this.getClass());
	}

	public Functor[] stages() {
		return new Functor[0];
	}

	abstract public void streaming(final JavaStreamingContext ssc, final Factors factors,
			final VoidFunction<JavaPairRDD<OUTK, OUTV>> handler);

	protected boolean saving(JavaPairRDD<OUTK, OUTV> r) {
		return true;
	}

	protected void traceCount(JavaPairRDD<?, ?> rdd, String prefix) {
		if (Calculator.debug && logger.isTraceEnabled() && !rdd.isEmpty()) {
			logger.trace("Count RDD............................");
			logger.trace(prefix + rdd.count());
		}
	}

	protected void traceCount(JavaPairRDD<?, ?> rdd, String prefix, double sd) {
		if (Calculator.debug && logger.isTraceEnabled() && !rdd.isEmpty()) {
			logger.trace("Count RDD............................");
			logger.trace(prefix + rdd.countApproxDistinct(sd));
		}
	}

	@SuppressWarnings("deprecation")
	protected void traceCount(JavaPairDStream<?, ?> stream, String prefix) {
		if (Calculator.debug && logger.isTraceEnabled()) {
			logger.trace("Count STREAM............................");
			stream.count().foreachRDD(rdd -> {
				if (!rdd.isEmpty()) logger.trace(prefix + rdd.reduce((c1, c2) -> c1 + c2));
				return null;
			});
		};
	}

	protected <K, V> void traceInfo(JavaPairRDD<K, V> rdd, Function2<K, V, String> func) {
		if (Calculator.debug && logger.isTraceEnabled() && !rdd.isEmpty()) rdd.foreach(t -> {
			logger.trace("Info RDD............................");
			logger.trace(func.call(t._1, t._2));
		});
	}

	@SuppressWarnings("deprecation")
	protected <K, V> void traceInfo(JavaPairDStream<K, V> stream, Function2<K, V, String> func) {
		if (Calculator.debug && logger.isTraceEnabled()) stream.foreachRDD(rdd -> {
			logger.trace("Info STREAM............................");
			traceInfo(rdd, func);
			return null;
		});
	}

	@SuppressWarnings("rawtypes")
	protected boolean check(JavaRDDLike... rdd) {
		for (JavaRDDLike r : rdd)
			if (null == r || r.isEmpty()) return false;
		return true;
	}

	@SuppressWarnings("rawtypes")
	protected boolean check(JavaDStreamLike... s) {
		for (JavaDStreamLike r : s)
			if (null == r) return false;
		return true;
	}
}
