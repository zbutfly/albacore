package net.butfly.albacore.calculus.utils;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.calculus.lambda.Func;
import scala.Tuple2;

public final class Reflections implements Serializable {
	private static final long serialVersionUID = 6337397752201899394L;

	private Reflections() {}

	public static <T> T get(Object obj, String field) {
		return get(obj, Reflections.getDeclaredField(obj.getClass(), field));
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(Object obj, Field field) {
		if (!field.isAccessible()) field.setAccessible(true);
		try {
			return (T) field.get(obj);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> void set(Object obj, String field, T value) {
		set(obj, Reflections.getDeclaredField(obj.getClass(), field), value);
	}

	public static <T> void set(Object obj, Field field, T value) {
		if (!field.isAccessible()) field.setAccessible(true);
		try {
			field.set(obj, value);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static Field[] getDeclaredFields(Class<?> clazz) {
		noneNull("", clazz);
		Map<String, Field> fields = new HashMap<String, Field>();
		while (clazz != null) {
			for (Field field : clazz.getDeclaredFields()) {
				if (fields.containsKey(field.getName())) continue;
				int mod = field.getModifiers();
				if (Modifier.isFinal(mod) || Modifier.isStatic(mod) || Modifier.isTransient(mod) || Modifier.isVolatile(mod)) continue;
				fields.put(field.getName(), field);
			}
			clazz = clazz.getSuperclass();
		}

		return fields.values().toArray(new Field[fields.size()]);
	}

	public static Field getDeclaredField(Class<?> clazz, String name) {
		noneNull("", clazz, name);
		while (clazz != null) {
			try {
				return clazz.getDeclaredField(name);
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			}
		}
		return null;
	}

	public static <T> T construct(String className, Object... parameters) {
		return construct(forClassName(className), parameters);
	}

	public static <T> T construct(final Class<T> cls, Object... parameters) {
		try {
			if (null == parameters || parameters.length == 0) return ConstructorUtils.invokeConstructor(cls);
			else return ConstructorUtils.invokeConstructor(cls, parameters);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> forClassName(String className) {
		try {
			return (Class<T>) Thread.currentThread().getContextClassLoader().loadClass(className);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * <p>
	 * Converts an array of {@code Object} in to an array of {@code Class}
	 * objects. If any of these objects is null, a null element will be inserted
	 * into the array.
	 * </p>
	 *
	 * <p>
	 * This method returns {@code null} for a {@code null} input array.
	 * </p>
	 *
	 * @param array
	 *            an {@code Object} array
	 * @return a {@code Class} array, {@code null} if null array input
	 * @since 2.4
	 */
	public static Class<?>[] toClass(final Object... array) {
		if (array == null) return null;
		else if (array.length == 0) return new Class[0];
		final Class<?>[] classes = new Class[array.length];
		for (int i = 0; i < array.length; i++)
			classes[i] = array[i] == null ? null : array[i].getClass();
		return classes;
	}

	public static boolean isAny(Class<?> cl, Class<?>... target) {
		for (Class<?> t : target)
			if (t.isAssignableFrom(cl)) return true;
		return false;
	}

	@SuppressWarnings("unchecked")
	public static <E> Class<E> resolveGenericParameter(final Type implType, final Class<?> declareClass, final String genericParamName) {
		return (Class<E>) resolveGenericParameters(implType, declareClass).get(genericParamName);
	}

	private static Map<String, Class<?>> resolveGenericParameters(final Type implType, final Class<?> declareClass) {
		Map<String, Class<?>> types = new HashMap<>();
		TypeVariable<?>[] vv = declareClass.getTypeParameters();
		for (TypeVariable<?> v : vv) {
			types.put(v.getName(), (Class<?>) TypeToken.of(implType).resolveType(v).getRawType());
		} ;
		return types;
	}

	public static <T, K, V> Map<K, V> transMapping(Collection<T> list, Func<T, Tuple2<K, V>> mapping) {
		Map<K, V> map = new HashMap<>();
		list.forEach(t -> {
			Tuple2<K, V> e = mapping.call(t);
			map.put(e._1, e._2);
		});
		return map;
	}

	@SafeVarargs
	public static <T, R> List<R> transform(Func<T, R> trans, T... original) {
		if (original == null) return null;
		List<R> r = new ArrayList<>(original.length);
		for (T t : original) {
			r.add(trans.call(t));
		}
		return r;
	}

	public static <T, R> List<R> transform(Collection<T> original, Func<T, R> trans) {
		if (original == null) return null;
		List<R> r = new ArrayList<>(original.size());
		original.forEach(new Consumer<T>() {
			@Override
			public void accept(T o) {
				r.add(trans.call(o));
			}
		});
		return r;
	}

	public static <T, R> List<R> transform(Iterable<T> original, Func<T, R> trans) {
		if (original == null) return null;
		return transform(original.iterator(), trans);
	}

	public static <T, R> List<R> transform(Iterator<T> original, Func<T, R> trans) {
		if (original == null) return null;
		List<R> r = new ArrayList<>();
		while (original.hasNext()) {
			r.add(trans.call(original.next()));
		}
		return r;
	}

	public static boolean anyNull(Object... value) {
		for (Object v : value)
			if (null == v) return true;
		return false;
	}

	public static void noneNull(String msg, Object... value) {
		for (Object v : value)
			if (null == v) throw new IllegalArgumentException(msg);
	}

	public static boolean anyEmpty(Object... value) {
		for (Object v : value) {
			if (null == v) return true;
			if (v.getClass().isArray() && Array.getLength(v) == 0) return true;
			if (v instanceof CharSequence && ((CharSequence) v).length() == 0) return true;
		}
		return false;
	}

	public static void copy(Object src, Object dst) {
		for (Field f : getDeclaredFields(src.getClass())) {
			Field f1 = getDeclaredField(dst.getClass(), f.getName());
			if (null != f1) try {
				f1.setAccessible(true);
				f.setAccessible(true);
				f1.set(dst, f.get(src));
			} catch (IllegalArgumentException | IllegalAccessException e) {}
		}
	}

	@SuppressWarnings("unchecked")
	public static <V> V invoke(Object object, String methodName, Object... args) {
		try {
			if (Class.class.isAssignableFrom(object.getClass())) return (V) MethodUtils.invokeStaticMethod((Class<?>) object, methodName,
					args);
			else return (V) MethodUtils.invokeMethod(object, methodName, args);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException(ex.getTargetException());
		}
	}

	public static <A extends Annotation> List<A> multipleAnnotation(Class<A> a, Class<? extends Annotation> ma, Class<?>... cc) {
		List<A> list = new ArrayList<>();
		if (null != cc && cc.length > 0) for (Class<?> c : cc) {
			if (null == c) continue;
			if (c.isAnnotationPresent(ma)) {
				A[] v = Reflections.invoke(c.getAnnotation(ma), "value");
				if (null != v) list.addAll(Arrays.asList(v));
			} else if (c.isAnnotationPresent(a)) list.add(c.getAnnotation(a));
			list.addAll(multipleAnnotation(a, ma, c.getSuperclass()));
			list.addAll(multipleAnnotation(a, ma, c.getInterfaces()));
		}
		return list;
	}
}
