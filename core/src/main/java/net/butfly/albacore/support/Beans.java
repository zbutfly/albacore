package net.butfly.albacore.support;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Map;

public abstract class Beans<T extends Beans<T>> implements Serializable, Comparable<Beans<T>>, Cloneable {
	private static final long serialVersionUID = 1L;

	public static <B extends Beans<B>> Object get(B bean, String propertyName) {
		return null;
	}

	public static <B extends Beans<B>> void set(B bean, String propertyName, Object value) {}

	public static <B extends Beans<B>> Field parse(Class<B> bean, String propertyName) {
		return null;
	}

	public static <B extends Beans<B>> Map<String, Field> parse(Class<B> bean) {
		return null;
	}

	@Override
	public int compareTo(Beans<T> o) {
		Comparator<Beans<T>> comp = new Comparator<Beans<T>>() {

			@Override
			public int compare(Beans<T> c1, Beans<T> c2) {
				return c1.compareTo(c2);
			}
		};
		return comp.compare(this, o);
	}
}
