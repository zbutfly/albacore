package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.butfly.albacore.support.BeanMap;
import net.butfly.albacore.support.CloneSupport;
import net.butfly.albacore.support.EnumSupport;
import net.butfly.albacore.support.GenericEnumSupport;
import net.butfly.albacore.support.ObjectSupport;

import org.apache.commons.lang3.NotImplementedException;

public class ObjectUtils {
	private ObjectUtils() {}

	@SuppressWarnings("unchecked")
	public static <T> T deepClone(T object) {
		if (object == null) return null;
		if (object instanceof ObjectSupport) return (T) ((ObjectSupport<?>) object).deepClone();
		return object;

	}

	@SuppressWarnings("rawtypes")
	public static <T> boolean equals(T o1, T o2) {
		if (null == o1 && null == o2) return true;
		if (null == o1 || null == o2) return false;
		if (o1.getClass().isPrimitive()) return o1 == o2;
		if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass())) return o1.equals(o2);
		if (o1.getClass().isArray() && o2.getClass().isArray())
			return arrayComparator.compare((Object[]) o1, (Object[]) o2) == 0;
		if (Map.class.isAssignableFrom(o1.getClass()) && Map.class.isAssignableFrom(o2.getClass()))
			return mapComparator.compare((Map) o1, (Map) o2) == 0;
		if (Iterable.class.isAssignableFrom(o1.getClass()) && Iterable.class.isAssignableFrom(o2.getClass()))
			return iterableComparator.compare((Iterable) o1, (Iterable) o2) == 0;

		if (!o1.getClass().equals(o2.getClass())) return false;
		return mapComparator.compare(new BeanMap<T>(o1), new BeanMap<T>(o2)) == 0;
	}

	public static <S, D> D clone(S source, Class<D> clazz) {
		D dst = null;
		try {
			dst = clazz.newInstance();
		} catch (InstantiationException ex) {
			return null;
		} catch (IllegalAccessException ex) {
			return null;
		}
		copyByProperties(dst, source);
		return dst;
	}

	public static <O> Map<String, Object> toMap(O obj) {
		Map<String, Object> map = new HashMap<String, Object>();
		Map<String, Method[]> properties = GenericUtils.getAllProperties(obj.getClass());
		for (String name : properties.keySet()) {
			try {
				Method getter = properties.get(name)[CloneSupport.METHOD_GET];
				if (null != getter) {
					Object v = getter.invoke(obj);
					if (null != v) {
						map.put(name, v);
					}
				}
			} catch (IllegalArgumentException e) {} catch (IllegalAccessException e) {} catch (InvocationTargetException e) {}
		}
		return map;
	}

	@SuppressWarnings("unchecked")
	public static <D extends CloneSupport<D>> D[] copy(CloneSupport<?>[] src, Class<D> clazz) {
		if (null == src) return null;
		D[] r = (D[]) Array.newInstance(clazz, src.length);
		for (int i = 0; i < src.length; i++) {
			r[i] = clone(src[i], clazz);
		}
		return r;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Object castValue(Object value, Class<?> setClass, Class<?> getClass) {
		// 枚举类型和integer的互换
		if (isEnum(setClass) && isInteger(getClass)) {
			return EnumUtils.valueOfGeneric((Class<GenericEnumSupport>) setClass, ((Number) value).intValue());
		} else if (isInteger(setClass) && isEnum(getClass)) return ((EnumSupport<?>) value).value();
		// String和byte[]的互换
		else if (isBytes(setClass) && isString(getClass)) return ByteUtils.hex2byte((String) value);
		else if (isString(setClass) && isBytes(getClass)) return ByteUtils.byte2hex((byte[]) value);
		// integer和 String的互换
		else if (isString(setClass) && isInteger(getClass)) return Integer.toString(((Number) value).intValue());
		else if (isInteger(setClass) && isString(getClass)) return Integer.parseInt(value.toString());
		else { // TODO: deeply copy, be done later.
				// 其他直接转换
			return value;
		}

	}

	private static boolean isInteger(Class<?> clazz) {
		return int.class.equals(clazz) || Integer.class.equals(clazz);
	}

	private static boolean isString(Class<?> clazz) {
		return String.class.equals(clazz);
	}

	private static boolean isBytes(Class<?> clazz) {
		return byte[].class.equals(clazz);
	}

	private static boolean isEnum(Class<?> clazz) {
		return GenericEnumSupport.class.isAssignableFrom(clazz);
	}

	public static void copyByField(CloneSupport<?> dst, CloneSupport<?> src) {
		Map<String, Field> dmap = GenericUtils.getAllFields(dst.getClass());
		Map<String, Field> smap = GenericUtils.getAllFields(src.getClass());
		for (String name : smap.keySet()) {
			Field fsrc = smap.get(name);
			try {
				Object value = fsrc.get(src);
				if (null == value) {
					continue;
				}
				Field fdst = dmap.get(name);
				if (null == fdst) {
					continue;
				}
				fdst.set(dst, ObjectUtils.castValue(value, fdst.getType(), fsrc.getType()));
			} catch (IllegalArgumentException ex) {} catch (IllegalAccessException ex) {} catch (SecurityException ex) {}
		}
	}

	public static <S, D> void copyByProperties(S dst, D src) {
		Map<String, Method[]> dmap = GenericUtils.getAllProperties(dst.getClass());
		Map<String, Method[]> smap = GenericUtils.getAllProperties(src.getClass());
		for (String name : smap.keySet()) {
			try {
				Method fsrc = smap.get(name)[CloneSupport.METHOD_GET];
				if (null == fsrc) continue;
				Object value = fsrc.invoke(src);
				if (null == value) continue;

				Method[] fs = dmap.get(name);
				if (null == fs) {
					continue;
				}
				Method fdst = fs[CloneSupport.METHOD_SET];
				if (null == fdst) {
					continue;
				}
				fdst.invoke(dst, ObjectUtils.castValue(value, fdst.getParameterTypes()[0], fsrc.getReturnType()));
			} catch (IllegalArgumentException ex) {} catch (IllegalAccessException ex) {} catch (SecurityException ex) {} catch (InvocationTargetException e) {}
		}
	}

	@SuppressWarnings("rawtypes")
	public static <T> int compare(T o1, T o2) {
		if (null == o1 && null == o2) return 0;
		if (null == o1) return -1;
		if (null == o2) return 1;

		if (o1.getClass().isPrimitive()) throw new NotImplementedException("Comparation between primitive objects is not implemented.");
		if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass()))
			return numberComparator.compare((Number) o1, (Number) o2);
		if (o1.getClass().isArray() && o2.getClass().isArray()) return arrayComparator.compare((Object[]) o1, (Object[]) o2);
		if (Map.class.isAssignableFrom(o1.getClass()) && Map.class.isAssignableFrom(o2.getClass()))
			return mapComparator.compare((Map) o1, (Map) o2);
		if (Iterable.class.isAssignableFrom(o1.getClass()) && Iterable.class.isAssignableFrom(o2.getClass()))
			return iterableComparator.compare((Iterable) o1, (Iterable) o2);

		if (!o1.getClass().equals(o2.getClass()))
			throw new NotImplementedException("Comparation between objects with different classes is not implemented.");
		return mapComparator.compare(new BeanMap<T>(o1), new BeanMap<T>(o2));
	}

	private static final Comparator<Number> numberComparator = new Comparator<Number>() {
		@Override
		public int compare(Number o1, Number o2) {
			throw new NotImplementedException("Comparation between Numbers is not implemented.");
		}
	};
	private static final Comparator<Iterable<?>> iterableComparator = new Comparator<Iterable<?>>() {
		@Override
		public int compare(Iterable<?> o1, Iterable<?> o2) {
			if (null == o1 && null == o2) return 0;
			if (null == o1) return -1;
			if (null == o2) return 1;
			Iterator<?> it1 = o1.iterator(), it2 = o2.iterator();
			while (it1.hasNext() && it2.hasNext()) {
				int r = ObjectUtils.compare(it1.next(), it2.next());
				if (r != 0) return r;
			}
			if (it1.hasNext()) return 1;
			if (it2.hasNext()) return -1;
			return 0;
		}
	};

	private static final Comparator<Object[]> arrayComparator = new Comparator<Object[]>() {
		@Override
		public int compare(Object[] o1, Object[] o2) {
			if (null == o1 && null == o2) return 0;
			if (null == o1) return -1;
			if (null == o2) return 1;
			for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
				int r = ObjectUtils.compare(o1[i], o2[i]);
				if (r != 0) return r;
			}
			if (o1.length > o2.length) return 1;
			if (o1.length < o2.length) return -1;
			return 0;
		}
	};

	private static final Comparator<Map<?, ?>> mapComparator = new Comparator<Map<?, ?>>() {
		@Override
		public int compare(Map<?, ?> o1, Map<?, ?> o2) {
			if (null == o1 && null == o2) return 0;
			if (null == o1) return -1;
			if (null == o2) return 1;
			return iterableComparator.compare(o1.entrySet(), o2.entrySet());
		}
	};
}
