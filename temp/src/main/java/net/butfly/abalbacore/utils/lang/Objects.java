package net.butfly.abalbacore.utils.lang;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public final class Objects {
	public static <T> T get(T target, String prop) {
		return null;
	}

	public static <T, V> void set(T target, String prop, V value) {}

	public static String toString(Object target) {
		return null == target ? null : flatten(simplify(target)).toString();
	}

	/**
	 * Conver an object (target) to an multilayer map, each layer which has
	 * string key and "simple" value. The word "simlpe" means types include:
	 * <ol>
	 * <li>primitive</li>
	 * <li>string</li>
	 * <li>boxed primitive</li>
	 * <li>number</li>
	 * </ol>
	 * and any combination (<code>Array</code> or <code>Map</code>) of "simple".
	 * 
	 * @param target
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Object simplify(Object target) {
		if (null == target) return null;
		Class<?> clazz = target.getClass();
		if (Void.class.equals(clazz)) return null;
		if (isPrimitive(clazz)) return target;
		if (CharSequence.class.isAssignableFrom(clazz)) return target.toString();

		// number, return unboxed value or large string (for big number)
		if (Number.class.isAssignableFrom(clazz)) {
			Object v = getValueField(target);
			if (null != v) return v;
			try {
				Method m = clazz.getDeclaredMethod("get");
				int mod = m.getModifiers();
				if (!Modifier.isStatic(mod) && Modifier.isPublic(mod) && m.getReturnType().isPrimitive())
					return m.invoke(target);
			} catch (NoSuchMethodException e) {} catch (SecurityException e) {} catch (IllegalArgumentException e) {} catch (IllegalAccessException e) {} catch (InvocationTargetException e) {}
			return target.toString();
		}

		// array or iterable, return array.
		if (clazz.isArray()) {
			int len = Array.getLength(target);
			Object[] result = new Object[len];
			for (int i = 0; i < len; i++)
				result[i] = simplify(Array.get(target, i));
			return result;
		}
		if (Collection.class.isAssignableFrom(clazz)) return ((Collection) target).toArray();
		if (Iterable.class.isAssignableFrom(clazz)) {
			List l = new ArrayList();
			Iterator it = ((Iterable) target).iterator();
			while (it.hasNext())
				l.add(simplify(it.next()));
			return l.toArray();
		}

		// map, return map.
		if (Map.class.isAssignableFrom(clazz)) {
			Map<String, Object> result = new HashMap<String, Object>();
			Set<Entry> set = ((Map) target).entrySet();
			for (Entry e : set) {
				Object key = e.getKey();
				if (null == key || !CharSequence.class.isAssignableFrom(key.getClass()))
					throw new IllegalArgumentException("Only map with string key can be flattend.");
				result.put(key.toString(), simplify(e.getValue()));
			}
			return result;
		}

		// object, return map.
		Map<String, Object> result = new HashMap<String, Object>();
		for (FieldWrap f : FieldWrap.getAllFields(target.getClass()))
			result.put(f.getName(), simplify(f.getValue(target)));
		return result;
	}

	/**
	 * Convert an object to flattened map, which describing layer in key (string
	 * value splitted by "," and indexed by "[]".
	 * 
	 * @param target
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Object flatten(Object target) {
		if (null == target) return null;
		Class<?> clazz = target.getClass();
		if (Void.class.equals(clazz)) return null;
		if (isPrimitive(clazz)) return target;
		if (CharSequence.class.isAssignableFrom(clazz)) return target.toString();

		if (Map.class.isAssignableFrom(clazz)) return flattenMap((Map<String, ?>) (simplify(target)));

		// array or iterable, return array.
		if (clazz.isArray()) {
			Map<String, Object> result = new HashMap<String, Object>();
			for (int i = 0; i < Array.getLength(target); i++)
				result.put("[" + i + "]", simplify(Array.get(target, i)));
			return flattenMap(result);
		}

		return flatten(simplify(target));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, ?> flattenMap(Map<String, ?> target) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (String key : target.keySet()) {
			Object value = target.get(key);
			if (null == value) result.put(key, null);
			else if (Map.class.isAssignableFrom(value.getClass())) {
				// XXX: check if key type is string or CharSequence.
				Map<String, ?> submap = flattenMap((Map<String, ?>) value);
				for (String subkey : submap.keySet())
					result.put(key.toString() + (subkey.matches("\\[[\\d]+\\]") ? "" : ".") + subkey, submap.get(subkey));
			} else if (value.getClass().isArray()) for (int i = 0; i < Array.getLength(value); i++)
				result.put(key.toString() + "[" + i + "]", Array.get(value, i));
			else result.put(key.toString(), value);
		}
		return result;
	}

	private static boolean isPrimitive(Class<?> clazz) {
		return clazz.isPrimitive() || Character.class.equals(clazz) || Boolean.class.equals(clazz) || Byte.class.equals(clazz)
				|| Short.class.equals(clazz) || Integer.class.equals(clazz) || Long.class.equals(clazz)
				|| Float.class.equals(clazz) || Double.class.equals(clazz);
	}

	private static Object getValueField(Object target) {
		try {
			Field f = target.getClass().getDeclaredField("value");
			int mod = f.getModifiers();
			if (!Modifier.isStatic(mod) && Modifier.isPrivate(mod)) {
				boolean flag = f.isAccessible();
				try {
					f.setAccessible(true);
					if (f.getType().isPrimitive()) return f.get(target);
				} finally {
					f.setAccessible(flag);
				}
			}
		} catch (NoSuchFieldException e) {} catch (SecurityException e) {} catch (IllegalArgumentException e) {} catch (IllegalAccessException e) {}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T> T castStringToSimple(CharSequence str, Class<T> targetClass) {
		if (null == targetClass || null == str || Void.class.isAssignableFrom(targetClass)) return null;
		if (String.class.isAssignableFrom(targetClass)) return (T) str.toString();
		if (Number.class.isAssignableFrom(targetClass) || byte.class.equals(targetClass) || short.class.equals(targetClass)
				|| int.class.equals(targetClass) || long.class.equals(targetClass) || float.class.equals(targetClass)
				|| double.class.equals(targetClass))
			try {
			return (T) NumberFormat.getInstance().parse(str.toString());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		if (Character.class.equals(targetClass) || char.class.equals(targetClass))
			return (T) (str.length() == 0 ? null : str.charAt(0));
		if (boolean.class.equals(targetClass) || Boolean.class.equals(targetClass)) return (T) new Boolean(str.toString());
		throw new UnsupportedOperationException(targetClass.getName() + " casting from string is not supported.");
	}

	// Query string style string utils.
	public static <T> T fromQueryString(String queryString, Class<T> targetClass) {
		if (null == queryString) return null;
		if ("".equals(queryString)) return null;
		T target;
		try {
			target = targetClass.newInstance();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		Map<String, String> props = splitQueryString(queryString);
		for (Entry<String, String> e : props.entrySet()) {
			FieldWrap f = new FieldWrap(targetClass, e.getKey());
			if (null != f) f.set(target, castStringToSimple(e.getValue(), f.getType()));
		}
		return target;
	}

	public static String toQueryString(Object target) {
		return null == target ? null : flatten(simplify(target)).toString();
	}

	private static Map<String, String> splitQueryString(String queryString) {
		if (null == queryString) return null;
		Map<String, String> r = new HashMap<String, String>();
		for (String seg : queryString.split("\\&"))
			if (!"".equals(seg)) {
				String[] pair = seg.split("=", 2);
				switch (pair.length) {
				case 2:
					r.put(decode(pair[0]), decode(pair[1]));
					break;
				case 1:
					r.put(decode(pair[0]), null);
					break;
				}
			}
		return r;
	}

	private static String decode(String string) {
		try {
			return URLDecoder.decode(string.replace("+", "%2B"), Charset.defaultCharset().name());
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

}
