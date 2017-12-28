package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Annotations extends Utils {
	/**
	 * The set of method that annotation classes inherit, and should be avoided when toString()ing an annotation class.
	 */
	private static final Set<String> INHERITED_ANNOTATION_METHODS = new HashSet<String>(Arrays.asList("toString", "equals", "hashCode",
			"annotationType", "getClass"));

	/**
	 * <p>
	 * A better (more concise) toString method for annotation types that yields a String that should look more like the actual usage of the
	 * annotation in a class. The String produced is similar to that produced by calling toString() on the annotation directly, with the
	 * following differences:
	 * </p>
	 *
	 * <ul>
	 * <li>Uses the classes simple name instead of it's fully qualified name.</li>
	 * <li>Only outputs attributes that are set to non-default values.</li>
	 *
	 * <p>
	 * If, for some unforseen reason, an exception is thrown within this method it will be caught and the return value will be
	 * {@code ann.toString()}.
	 *
	 * @param ann
	 *            the annotation to convert to a human readable String
	 * @return a human readable String form of the annotation and it's attributes
	 */
	public static String toString(Annotation ann) {
		try {
			Class<? extends Annotation> type = ann.annotationType();
			StringBuilder builder = new StringBuilder(128);
			builder.append("@");
			builder.append(type.getSimpleName());

			boolean appendedAnyParameters = false;
			Method[] methods = type.getMethods();
			for (Method method : methods) {
				if (!INHERITED_ANNOTATION_METHODS.contains(method.getName())) {
					Object defaultValue = method.getDefaultValue();
					Object actualValue = method.invoke(ann);

					// If we have arrays, they have to be treated a little
					// differently
					Object[] defaultArray = null, actualArray = null;
					if (Object[].class.isAssignableFrom(method.getReturnType())) {
						defaultArray = (Object[]) defaultValue;
						actualArray = (Object[]) actualValue;
					}

					// Only print an attribute if it isn't set to the default
					// value
					if ((defaultArray != null && !Arrays.equals(defaultArray, actualArray)) || (defaultArray == null && !actualValue.equals(
							defaultValue))) {
						builder.append(appendedAnyParameters ? ", " : "(").append(method.getName()).append("=");
						builder.append(actualArray != null ? Arrays.toString(actualArray) : actualValue);
						appendedAnyParameters = true;
					}
				}
			}
			if (appendedAnyParameters) builder.append(")");
			return builder.toString();
		} catch (Exception e) {
			return ann.toString();
		}
	}

	public static <A extends Annotation> List<A> multipleAnnotation(Class<A> a, Class<? extends Annotation> ma, Class<?>... cc) {
		List<A> list = new ArrayList<A>();
		if (null != cc && cc.length > 0) for (Class<?> c : cc) {
			if (null == c) continue;
			if (c.isAnnotationPresent(ma)) {
				A[] v;
				try {
					v = Reflections.invoke(c.getAnnotation(ma), "value");
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				if (null != v) list.addAll(Arrays.asList(v));
			} else if (c.isAnnotationPresent(a)) list.add(c.getAnnotation(a));
			list.addAll(multipleAnnotation(a, ma, c.getSuperclass()));
			list.addAll(multipleAnnotation(a, ma, c.getInterfaces()));
		}
		return list;
	}
}