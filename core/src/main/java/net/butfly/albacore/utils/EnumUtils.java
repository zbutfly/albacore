package net.butfly.albacore.utils;

import java.lang.reflect.Method;

public class EnumUtils extends UtilsBase {
	public static final String ENUM_VALUE_METHOD_NAME = "value";

	@SuppressWarnings("unchecked")
	public static byte value(Enum<? extends Enum<?>> enumObject) {
		if (enumObject == null) throw new RuntimeException("Null could not be parsed as enum.");
		Method valueMethod = getValueMethod(enumObject.getClass());
		if (null == valueMethod) return (byte) enumObject.ordinal();
		else try {
			return (byte) valueMethod.invoke(enumObject);
		} catch (Exception e) {
			throw new RuntimeException("Failure on parsing as enum.", e);
		}
	}

	enum temp {
		a, b, c
	}

	public static <E extends Enum<E>> E parse(Class<E> type, byte enumValue) {
		E[] enums = type.getEnumConstants();
		Method valueMethod = getValueMethod(type);
		if (null == valueMethod) return enums[enumValue];// use ordinal;
		else for (E e : enums)
			try {
				if (enumValue == (byte) valueMethod.invoke(e)) return e;
			} catch (Exception ex) {
				throw new RuntimeException("Failure on parsing as enum.", ex);
			}
		return null;
	}

	private static <E extends Enum<E>> Method getValueMethod(Class<E> enumClass) {
		try {
			Method valueMethod = enumClass.getDeclaredMethod(ENUM_VALUE_METHOD_NAME);
			if (null == valueMethod) valueMethod = enumClass.getDeclaredMethod("getValue");
			return valueMethod;
		} catch (Exception e) {
			return null;
		}
	}
}
