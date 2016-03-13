package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;

@SuppressWarnings("unchecked")
public final class Functors implements Serializable {
	private static final long serialVersionUID = -3712903710207597570L;
	private Map<Class<? extends Functor<?>>, JavaPairDStream<String, ? extends Functor<?>>> streamings = new HashMap<>();
	private Map<Class<? extends Functor<?>>, JavaPairRDD<String, ? extends Functor<?>>> stocking = new HashMap<>();

	public <F extends Functor<F>> void add(Class<? extends Functor<?>> functor, JavaPairDStream<String, F> dstr) {
		this.streamings.put(functor, dstr);
	}

	public void add(Class<? extends Functor<?>> functor, JavaPairRDD<String, ? extends Functor<?>> rdd) {
		this.stocking.put(functor, rdd);
	}

	@SuppressWarnings("deprecation")
	public <F extends Functor<F>> JavaPairRDD<String, F> get(Class<F> functor) {
		JavaPairDStream<String, F> s = (JavaPairDStream<String, F>) streamings.get(functor);
		JavaPairRDD<String, F> r = (JavaPairRDD<String, F>) stocking.get(functor);
		if (s == null) return r;
		if (r == null) return stockize(s);
		JavaPairRDD<String, F>[] rr = new JavaPairRDD[] { r };
		s.foreachRDD((Function<JavaPairRDD<String, F>, Void>) rdd -> {
			rr[0] = rr[0].union(rdd);
			return null;
		});
		return rr[0];
	}

	@Deprecated
	public static <F extends Functor<F>> JavaPairRDD<String, F> stockize(JavaPairDStream<String, F> stream) {
		JavaPairRDD<String, F>[] r = new JavaPairRDD[] { null };
		stream.foreachRDD(rdd -> {
			if (r[0] == null) r[0] = rdd;
			else r[0] = r[0].union(rdd);
			return null;
		});
		return r[0];
	}

	@Deprecated
	public static long count(JavaDStreamLike<?, ?, ?> streaming) {
		long[] i = new long[] { 0 };
		streaming.count().foreachRDD(rdd -> {
			i[0] += rdd.reduce((v1, v2) -> v1 + v2);
			return null;
		});
		return i[0];
	}

}
