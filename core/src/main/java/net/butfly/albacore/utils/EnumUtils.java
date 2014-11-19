package net.butfly.albacore.utils;

import java.io.Serializable;
import java.lang.reflect.Method;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.support.EnumSupport;
import net.butfly.albacore.support.GenericEnumSupport;

@SuppressWarnings("unchecked")
public class EnumUtils {

	private EnumUtils() {}

	public static <V extends Serializable, E extends GenericEnumSupport<E, V>> E valueOfGeneric(Class<E> clazz, V value) {
		if (null == value) return null;
		try {
			E[] enums = (E[]) clazz.getMethod("values").invoke(clazz);
			for (E e : enums)
				if (value.equals(e.value())) return e;
			return null;
		} catch (Exception e) {
			throw new SystemException("", "Parameter should be EnumSupport class.", e);
		}
	}

	@SuppressWarnings("rawtypes")
	public static <E extends EnumSupport> E valueOf(Class<E> clazz, int value) {
		try {
			E[] enums = (E[]) clazz.getMethod("values").invoke(clazz);
			for (E e : enums)
				if (value == e.value()) return e;
			return null;
		} catch (Exception e) {
			throw new SystemException("", "Parameter should be EnumSupport class.", e);
		}
	}

	public static <E extends EnumSupport<?>> E[] values(Class<E> clazz) {
		try {
			return (E[]) clazz.getMethod("values", new Class<?>[0]).invoke(null, new Object[0]);
		} catch (Exception e) {
			throw new SystemException("", "Parameter should be EnumSupport class.", e);
		}
	}

	public static <E extends Enum<E>> E valueOfEnum(Class<E> clazz, int value) {
		try {
			E[] enums = (E[]) clazz.getMethod("values").invoke(clazz);
			Method method;
			try {
				method = clazz.getMethod(GenericEnumSupport.ENUM_VALUE_METHOD_NAME);
			} catch (Exception e) {
				method = null;
			}
			if (!int.class.equals(method.getReturnType()) && !Number.class.isAssignableFrom(method.getReturnType()))
				method = null;

			for (E e : enums) {
				int v = null == method ? e.ordinal() : ((Number) method.invoke(e)).intValue();
				if (v == value) return e;
			}
			return null;
		} catch (Exception e) {
			throw new SystemException("", e);
		}
	}

	public static <E extends Enum<E>> E valueOf(Class<E> clazz, String name) {
		try {
			return Enum.valueOf(clazz, name);
		} catch (Exception e) {
			return null;
		}
	}
}
