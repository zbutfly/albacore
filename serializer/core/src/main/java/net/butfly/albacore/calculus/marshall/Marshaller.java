package net.butfly.albacore.calculus.marshall;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;

import net.butfly.albacore.serializer.modifier.Ignore;
import net.butfly.albacore.serializer.modifier.Key;
import net.butfly.albacore.serializer.modifier.PrimaryKey;
import net.butfly.albacore.serializer.modifier.Property;
import net.butfly.albacore.utils.Reflections;
import scala.Tuple2;

@SuppressWarnings("unchecked")
public class Marshaller<FK, VK, VV> implements Serializable {
	private static final long serialVersionUID = 6678021328832491260L;
	private static final CaseFormat DEFAULT_SRC_FORMAT = CaseFormat.LOWER_CAMEL;
	private static final CaseFormat DEFAULT_DST_FORMAT = CaseFormat.UPPER_UNDERSCORE;

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	private final Function<String, String> mapping;

	public Marshaller() {
		this(s -> DEFAULT_SRC_FORMAT.to(DEFAULT_DST_FORMAT, s));
	}

	public Marshaller(CaseFormat dstFormat) {
		this(s -> DEFAULT_SRC_FORMAT.to(dstFormat, s));
	}

	public Marshaller(CaseFormat srcFormat, CaseFormat dstFormat) {
		this(s -> srcFormat.to(dstFormat, s));
	}

	public Marshaller(Function<String, String> mapping) {
		super();
		this.mapping = mapping;
	}

	public FK unmarshallId(VK id) {
		return null == id ? null : (FK) id;
	}

	public <T> T unmarshall(VV from, Class<T> to) {
		return (T) from;
	}

	public VK marshallId(FK id) {
		return (VK) id;
	}

	public <T> VV marshall(T from) {
		return (VV) from;
	}

	public Comparator<FK> comparator() {
		throw new UnsupportedOperationException();
	}

	public String parseQualifier(Field f) {
		Reflections.noneNull("", f);
		return f.isAnnotationPresent(Property.class) ? f.getAnnotation(Property.class).value() : mapping.apply(f.getName());
	}

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

	public static <K, V> K key(V v) {
		try {
			return null == v ? null : (K) Marshaller.parseFirstOfAny(v.getClass(), Key.class)._1.get(v);
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
