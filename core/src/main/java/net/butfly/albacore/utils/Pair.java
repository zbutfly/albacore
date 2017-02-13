package net.butfly.albacore.utils;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

public final class Pair<T1, T2> implements Serializable, Entry<T1, T2> {
	private static final long serialVersionUID = 6995675769216721583L;
	private T1 v1;
	private T2 v2;

	public Pair() {}

	public Pair(T1 v1, T2 v2) {
		super();
		this.v1 = v1;
		this.v2 = v2;
	}

	public T1 v1() {
		return v1;
	}

	public T2 v2() {
		return v2;
	}

	public Pair<T1, T2> v1(T1 v1) {
		this.v1 = v1;
		return this;
	}

	public Pair<T1, T2> v2(T2 v2) {
		this.v2 = v2;
		return this;
	}

	@Override
	public String toString() {
		return "[" + v1 + "," + v2 + "]";
	}

	@Deprecated
	@Override
	public T1 getKey() {
		return v1;
	}

	@Deprecated
	@Override
	public T2 getValue() {
		return v2;
	}

	@Deprecated
	@Override
	public T2 setValue(T2 value) {
		return v2 = value;
	}

	public static <T1, T2> Collector<? super Pair<T1, T2>, ?, Map<T1, T2>> toMap() {
		return Collectors.toMap(p -> p.v1(), p -> p.v2());
	}

	public static <T1, T2> Map<T1, T2> collect(Stream<Pair<T1, T2>> s) {
		return s.collect(toMap());
	}

	public Tuple2<? super T1, ? super T2> tuple() {
		return new Tuple2<T1, T2>(v1, v2);
	}

	public static <T1, T2> Pair<T1, T2> of(T1 v1, T2 v2) {
		return new Pair<>(v1, v2);
	}

	public static <T1, T2> Pair<T1, T2> of(Class<? extends T1> c1, Class<? extends T2> c2) {
		return new Pair<>(null, null);
	}

	public static <T1, T2> Pair<T1, T2> of(Tuple2<? extends T1, ? extends T2> t) {
		return new Pair<>(t._1, t._2);
	}
}
