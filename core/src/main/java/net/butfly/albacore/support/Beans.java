package net.butfly.albacore.support;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Map;

public interface Beans<T extends Beans<T>> extends Serializable, Comparable<Beans<T>>, Cloneable {
	static <B extends Beans<B>> Object get(B bean, String propertyName) {
		return null;
	}

	static <B extends Beans<B>> void set(B bean, String propertyName, Object value) {}

	static <B extends Beans<B>> Field parse(Class<B> bean, String propertyName) {
		return null;
	}

	static <B extends Beans<B>> Map<String, Field> parse(Class<B> bean) {
		return null;
	}

	@Override
	default int compareTo(Beans<T> o) {
		Comparator<Beans<T>> comp = Comparator.naturalOrder();
		return comp.compare(this, o);
	}
}
