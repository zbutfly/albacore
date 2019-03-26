package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.util.Collection;

@SuppressWarnings("rawtypes")
public class Objects extends Utils {
	public static final int HASH_SEED = 17;
	public static final int HASH_OFFSET = 37;

	@SafeVarargs
	public static <T> T or(T one, T... ts) {
		if (null != one || ts == null || ts.length == 0) return one;
		for (T t : ts) if (null != t) return t;
		return null;
	}

	public static void noneNull(Object... target) {
		if (null == target) throw new NullPointerException();
		if (target.length == 0) return;
		for (Object o : target) if (null == o) throw new NullPointerException();
	}

	public static boolean isEmpty(Object target) {
		if (target == null) return true;
		Class<?> targetClass = target.getClass();
		if (String.class.equals(targetClass)) return target.toString().trim().length() == 0;
		if (targetClass.isArray()) return Array.getLength(target) == 0;
		if (Collection.class.isAssignableFrom(targetClass)) return ((Collection) target).size() == 0;
		return false;
	}

	public static void notEmpty(Object target) {
		if (isEmpty(target)) throw new IllegalArgumentException("Target should not be empty.");
	}

	@SuppressWarnings("unchecked")
	public <T> T[] junction(T[]... arrays) {
		int len = 0;
		for (T[] a : arrays) len += a.length;
		T[] result = (T[]) Array.newInstance(arrays.getClass().getComponentType().getComponentType(), len);
		int pos = 0;
		for (T[] a : arrays) {
			System.arraycopy(a, 0, result, pos, a.length);
			pos += a.length;
		}
		return result;
	}
}
