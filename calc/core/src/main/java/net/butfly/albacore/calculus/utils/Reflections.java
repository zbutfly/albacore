package net.butfly.albacore.calculus.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

import com.google.common.reflect.TypeToken;

public final class Reflections {
	private Reflections() {}

	public static Field[] getDeclaredFields(Class<?> clazz) {
		Map<String, Field> fields = new HashMap<String, Field>();
		while (clazz != null) {
			for (Field field : clazz.getDeclaredFields()) {
				if (fields.containsKey(field.getName())) continue;
				int mod = field.getModifiers();
				if (Modifier.isFinal(mod)) continue;
				if (Modifier.isStatic(mod)) continue;
				if (Modifier.isTransient(mod)) continue;
				if (Modifier.isVolatile(mod)) continue;

				fields.put(field.getName(), field);
			}
			clazz = clazz.getSuperclass();
		}

		return fields.values().toArray(new Field[fields.size()]);
	}

	public static <T> T construct(String className, Object... parameters) {
		Class<T> clazz = forClassName(className);
		return construct(clazz, parameters);
	}

	public static <T> T construct(final Class<T> cls, Object... parameters) {
		final Class<?> parameterTypes[] = toClass(parameters);
		return construct(cls, parameters, parameterTypes);
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

	public static void set(Object owner, Field field, Object value) throws IllegalArgumentException, IllegalAccessException {
		field.setAccessible(true);
		field.set(owner, value);
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
		};
		return types;
	}
}
