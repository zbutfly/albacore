package net.butfly.albacore.support;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Utils;

public final class Values extends Utils {
	public static final String NULL_STR = String.valueOf((Object) null);

	public static Class<?> is(Class<?> type) {
		if (null == type) return Void.class;
		for (Class<?> cl : VALUE_TYPES)
			if (cl.isAssignableFrom(type)) return cl;
		return null;
	}

	public static Class<?> is(Object value) {
		if (null == value) return Void.class;
		return is(value.getClass());
	}

	public static Class<?> isN(Class<?> type) {
		if (null == type) return null;
		for (Class<?> cl : NUMBER_TYPES)
			if (cl.isAssignableFrom(type)) return cl;
		return null;
	}

	public static Class<?> isN(Object value) {
		if (null == value) return null;
		return isN(value.getClass());
	}

	public static boolean isVs(Object value) {
		if (null == value) return false;
		Class<?> type = value.getClass();
		if (type.isArray() && null != is(type.getComponentType())) return true;
		if (Iterable.class.isAssignableFrom(type)) {
			Iterator<?> it = ((Iterable<?>) value).iterator();
			while (it.hasNext())
				if (null == is(it.next())) return false;
			return true;
		}
		return false;
	}

	public static String toString(Object value) {
		if (null == is(value)) throw new RuntimeException("Not simple value: " + value);
		if (null == value) return NULL_STR;
		if (Date.class.isAssignableFrom(value.getClass())) return String.valueOf(((Date) value).getTime());
		return String.valueOf(value);
	};

	@SuppressWarnings("unchecked")
	public static <T> T fromString(String str, Class<T> type) {
		if (null == str || str.length() == 0 || NULL_STR.equals(str)) return (T) null;
		final Class<?> vt = is(type);
		if (null == vt) throw new RuntimeException("Not simple type: " + type);
		if (Number.class.isAssignableFrom(type)) return Reflections.construct(type, str);
		return (T) FROM_STRING_METHOD.get(vt).apply(str);
	};

	/**
	 * <p>
	 * Converts the specified primitive Class object to its corresponding
	 * wrapper Class object.
	 * </p>
	 *
	 * <p>
	 * NOTE: From v2.2, this method handles {@code Void.TYPE}, returning
	 * {@code Void.TYPE}.
	 * </p>
	 *
	 * @param cls
	 *            the class to convert, may be null
	 * @return the wrapper class for {@code cls} or {@code cls} if {@code cls}
	 *         is not a primitive. {@code null} if null input.
	 * @since 2.1
	 */
	public static Class<?> primitiveToWrapper(final Class<?> cls) {
		Class<?> convertedClass = cls;
		if (cls != null && cls.isPrimitive()) {
			convertedClass = primitiveWrapperMap.get(cls);
		}
		return convertedClass;
	}

	/**
	 * <p>
	 * Converts the specified wrapper class to its corresponding primitive
	 * class.
	 * </p>
	 *
	 * <p>
	 * This method is the counter part of {@code primitiveToWrapper()}. If the
	 * passed in class is a wrapper class for a primitive type, this primitive
	 * type will be returned (e.g. {@code Integer.TYPE} for
	 * {@code Integer.class}). For other classes, or if the parameter is
	 * <b>null</b>, the return value is <b>null</b>.
	 * </p>
	 *
	 * @param cls
	 *            the class to convert, may be <b>null</b>
	 * @return the corresponding primitive type if {@code cls} is a wrapper
	 *         class, <b>null</b> otherwise
	 * @see #primitiveToWrapper(Class)
	 * @since 2.4
	 */
	public static Class<?> wrapperToPrimitive(final Class<?> cls) {
		return wrapperPrimitiveMap.get(cls);
	}

	public static void main(String... args) {
		fromString("1234", int.class);
	}

	private static final Class<?>[] VALUE_TYPES = new Class[] { //
			/** String **/
			CharSequence.class, char.class, Character.class, //
			/** Number **/
			Number.class, int.class, byte.class, short.class, long.class, double.class, float.class,
			/** Bool **/
			boolean.class, Boolean.class, //
			/** Date **/
			Date.class, //
			/** null **/
			Void.class };

	private static Map<Class<?>, Converter<String, Object>> FROM_STRING_METHOD = new HashMap<>();

	static {
		FROM_STRING_METHOD.put(Void.class, s -> NULL_STR);
		FROM_STRING_METHOD.put(CharSequence.class, s -> s);
		FROM_STRING_METHOD.put(Character.class, s -> s.toCharArray()[0]);
		FROM_STRING_METHOD.put(char.class, s -> s.toCharArray()[0]);

		FROM_STRING_METHOD.put(byte.class, s -> new Byte(s).byteValue());
		FROM_STRING_METHOD.put(short.class, s -> new Short(s).shortValue());
		FROM_STRING_METHOD.put(int.class, s -> new Integer(s).intValue());
		FROM_STRING_METHOD.put(long.class, s -> new Long(s).longValue());
		FROM_STRING_METHOD.put(double.class, s -> new Double(s).doubleValue());
		FROM_STRING_METHOD.put(float.class, s -> new Float(s).floatValue());

		FROM_STRING_METHOD.put(boolean.class, s -> new Boolean(s).booleanValue());
		FROM_STRING_METHOD.put(Boolean.class, s -> new Boolean(s));
		FROM_STRING_METHOD.put(Date.class, s -> new Date(Long.parseLong(s)));
	}
	private static final Class<?>[] NUMBER_TYPES = new Class[] { //
			Number.class, int.class, byte.class, short.class, long.class, double.class, float.class };

	/**
	 * Maps primitive {@code Class}es to their corresponding wrapper
	 * {@code Class}.
	 */
	private static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<Class<?>, Class<?>>();

	static {
		primitiveWrapperMap.put(Boolean.TYPE, Boolean.TYPE);
		primitiveWrapperMap.put(Byte.TYPE, Byte.TYPE);
		primitiveWrapperMap.put(Character.TYPE, Character.TYPE);
		primitiveWrapperMap.put(Short.TYPE, Short.TYPE);
		primitiveWrapperMap.put(Integer.TYPE, Integer.TYPE);
		primitiveWrapperMap.put(Long.TYPE, Long.TYPE);
		primitiveWrapperMap.put(Double.TYPE, Double.TYPE);
		primitiveWrapperMap.put(Float.TYPE, Float.TYPE);
		primitiveWrapperMap.put(Void.TYPE, Void.TYPE);
	}

	/**
	 * Maps wrapper {@code Class}es to their corresponding primitive types.
	 */
	private static final Map<Class<?>, Class<?>> wrapperPrimitiveMap = new HashMap<Class<?>, Class<?>>();

	static {
		for (final Class<?> primitiveClass : primitiveWrapperMap.keySet()) {
			final Class<?> wrapperClass = primitiveWrapperMap.get(primitiveClass);
			if (!primitiveClass.equals(wrapperClass)) {
				wrapperPrimitiveMap.put(wrapperClass, primitiveClass);
			}
		}
	}
}
