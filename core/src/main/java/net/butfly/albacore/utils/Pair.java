package net.butfly.albacore.utils;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;

import net.butfly.albacore.utils.collection.Maps;

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
		return "<" + v1 + "," + v2 + ">";
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

	public static <T1, T2> Map<T1, T2> collect(Iterable<Pair<T1, T2>> s) {
		Map<T1, T2> m = Maps.of();
		for (Pair<T1, T2> p : s)
			m.putIfAbsent(p.v1, p.v2);
		return m;
	}

	public static <T1, T2> Pair<T1, T2> of(T1 v1, T2 v2) {
		return new Pair<>(v1, v2);
	}

	public static <T1, T2> Pair<T1, T2> of(Class<? extends T1> c1, Class<? extends T2> c2) {
		return new Pair<>(null, null);
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof Pair<?, ?> && Objects.equals(v1, ((Pair<?, ?>) other).v1) && Objects.equals(v2, ((Pair<?, ?>) other).v2);
	}

	@Override
	public int hashCode() {
		return hashCode(v1, v2);
	}

	public static int hashCode(Object... objs) {
		if (null == objs) return 0;
		if (objs.length == 0) return objs.hashCode();
		int hash = 0;
		for (int i = 0; i < objs.length; i++)
			if (null != objs[i]) hash = hash * 17 + (objs[i].hashCode() + i);
		return hash;
	}

	public static <K, V> Predicate<Pair<K, V>> notNull() {
		return p -> null != p && null != p.v1() && null != p.v2();
	}

	public static <K, V> Pair<K, V> notNull(Pair<K, V> origin) {
		return null == origin || null == origin.v1() || null == origin.v2() ? null : origin;
	}
}
