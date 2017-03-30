package net.butfly.albacore.support;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import com.google.common.primitives.Primitives;

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
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromString(String str, Class<T> type) {
		if (null == str || str.length() == 0 || NULL_STR.equals(str)) return (T) null;
		final Class<?> vt = is(type);
		if (null == vt) throw new RuntimeException("Not simple type: " + type);
		if (Number.class.isAssignableFrom(type)) return Reflections.construct(type, str);
		return (T) FROM_STRING_METHOD.get(vt).apply(str);
	}

	/**
	 * <p>
	 * Collections the specified wrapper class to its corresponding primitive
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
		try {
			return (Class<?>) cls.getField("TYPE").get(null);
		} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
			return null;
		}
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

	private static Map<Class<?>, Function<String, Object>> FROM_STRING_METHOD = new HashMap<>();

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

	public static int sizing(Object v) {
		if (null == v) return 0;
		if (v instanceof Iterable<?>) return sizing(((Iterable<?>) v).iterator());
		if (v instanceof Iterator<?>) {
			int s = 0;
			while (((Iterator<?>) v).hasNext())
				s += sizing(((Iterator<?>) v).next());
			return s;
		}
		Class<?> cl = v.getClass();
		if (Map.class.isAssignableFrom(cl)) return sizing(((Map<?, ?>) v).values());
		if (byte[].class.isAssignableFrom(cl)) return ((byte[]) v).length;
		int cc = sizingPrimitive(cl.getComponentType());
		if (cc >= 0) return cc;
		if (cl.isArray()) {
			cc = sizingPrimitive(cl.getComponentType());
			if (cc >= 0) return cc * Array.getLength(v);
			int l = 0;
			for (int i = 0; i < Array.getLength(v); i++)
				l += sizing(Array.get(v, i));
			return l;
		}
		if (CharSequence.class.isAssignableFrom(cl)) return ((CharSequence) v).toString().getBytes().length;
		return 0;
	}

	private static int sizingPrimitive(Class<?> cl) {
		if (cl.isPrimitive()) cl = Primitives.wrap(cl);
		try {
			Field f = cl.getField("BYTES");
			if (Modifier.isStatic(f.getModifiers()) && Modifier.isPublic(f.getModifiers()) && Modifier.isFinal(f.getModifiers())
					&& int.class.equals(f.getType())) return (int) f.get(null);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {}
		return -1;
	}
}
