package net.butfly.albacore.serder.support;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.serder.modifier.Ignore;
import net.butfly.albacore.serder.modifier.Key;
import net.butfly.albacore.serder.modifier.PrimaryKey;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;
import scala.Tuple2;

public final class Qualifiers extends Utils {
	@SafeVarargs
	public static <V> Tuple2<Field, ? extends Annotation> parseFirstOfAny(Class<V> c, Class<? extends Annotation>... annotation) {
		for (Field f : Reflections.getDeclaredFields(c))
			for (Class<? extends Annotation> a : annotation)
				if (f.isAnnotationPresent(a)) return new Tuple2<>(f, f.getAnnotation(a));
		return new Tuple2<>(null, null);
	}

	public static <V> Field keyField(Class<V> c) {
		return parseFirstOfAny(c, Key.class)._1;
	}

	public static <V> Field[] keyFields(Class<V> c) {
		return parseAll(c, Key.class);
	}

	public static <V> Field idField(Class<V> c) {
		return parseFirstOfAny(c, PrimaryKey.class)._1;
	}

	@SafeVarargs
	public static <V> Map<Field, Annotation> parseAllForAny(Class<V> c, Class<? extends Annotation>... annotation) {
		Map<Field, Annotation> fs = new HashMap<>();
		for (Field f : Reflections.getDeclaredFields(c))
			for (Class<? extends Annotation> a : annotation)
				if (f.isAnnotationPresent(a)) {
					fs.put(f, f.getAnnotation(a));
					break;
				}
		return fs;
	}

	public static <V> Field[] parseAll(Class<V> c, Class<? extends Annotation> a) {
		List<Field> fs = new ArrayList<>();
		for (Field f : Reflections.getDeclaredFields(c))
			if (f.isAnnotationPresent(a)) {
				fs.add(f);
				break;
			}
		return fs.toArray(new Field[fs.size()]);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> K key(V v) {
		try {
			return null == v ? null : (K) parseFirstOfAny(v.getClass(), Key.class)._1.get(v);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			return null;
		}
	}

	public static boolean enableDB(Field f) {
		if (f.isAnnotationPresent(Ignore.class)) return false;
		int m = f.getModifiers();
		if (Modifier.isTransient(m) || Modifier.isFinal(m) || Modifier.isStatic(m)) return false;
		return true;
	}
}
