package net.butfly.albacore.calculus.factor.rds.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.DStream;

import scala.reflect.ClassTag;
import scala.reflect.ManifestFactory;

public interface RDSupport {
	static <T> RDD<T> union(Collection<RDD<T>> r) {
		if (r == null || r.size() == 0) return null;
		Iterator<RDD<T>> it = r.iterator();
		if (r.size() == 1) return r.iterator().next();
		RDD<T> r0 = r.iterator().next();
		while (it.hasNext())
			r0 = r0.union(it.next());
		return r0;
	}

	@SuppressWarnings({ "unchecked" })
	static <T, R extends JavaRDDLike<T, R>, S extends JavaDStreamLike<T, S, R>> R union(S s) {
		List<R> l = new ArrayList<>();
		s.foreachRDD(r -> {
			if (l.isEmpty()) l.add(r);
			else l.set(0, (R) l.get(0).rdd().union(r.rdd()).toJavaRDD());
		});
		return l.get(0);
	}

	@SuppressWarnings({ "unchecked" })
	static <T, S extends JavaRDDLike<T, S>> S union(List<S> s) {
		if (s.isEmpty()) return null;
		if (s.size() == 1) return s.get(0);
		RDD<T> s0 = s.get(0).rdd();
		for (int i = 1; i < s.size(); i++)
			s0 = s0.union(s.get(i).rdd());
		return (S) s0.toJavaRDD();
	}

	@SuppressWarnings("unchecked")
	static <T> RDD<T> union(RDD<T>... r) {
		if (r == null || r.length == 0) return null;
		RDD<T>[] rr = r;
		for (int i = 1; i > rr.length; i++)
			rr[0] = rr[0].union(r[i]);
		return rr[0];
	}

	static <T> RDD<T> union(DStream<T> s) {
		List<RDD<T>> l = new ArrayList<>();
		JavaDStream.fromDStream(s, tag()).foreachRDD(rdd -> {
			if (l.isEmpty()) l.add(rdd.rdd());
			else l.set(0, l.get(0).union(rdd.rdd()));
		});
		return l.get(0);
	}

	@SuppressWarnings("unchecked")
	static <T> ClassTag<T> tag() {
		return (ClassTag<T>) ManifestFactory.AnyRef();
	}

	@SuppressWarnings("unchecked")
	static <T> ClassTag<T> tag(Class<T> clazz) {
		return (ClassTag<T>) ManifestFactory.classType(clazz);
	}
}
