package net.butfly.albacore.utils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class EnumUtils extends UtilsBase {
	public static final String ENUM_VALUE_METHOD_NAME = "value";
	public static final String ENUM_VALUE_PARSE_NAME = "parse";

	public static byte value(Enum<?> enumObject) {
		if (enumObject == null) throw new RuntimeException("Null could not be parsed as enum.");
		Method valueMethod = null;
		try {
			valueMethod = enumObject.getClass().getDeclaredMethod(ENUM_VALUE_METHOD_NAME);
		} catch (Exception e) {}
		if (null == valueMethod) try {
			valueMethod = enumObject.getClass().getDeclaredMethod("getValue");
		} catch (Exception e) {}
		if (null == valueMethod) return (byte) enumObject.ordinal();
		try {
			return (byte) valueMethod.invoke(enumObject);
		} catch (Exception e) {
			throw new RuntimeException("Failure on parsing as enum.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <E extends Enum<E>> E parse(Class<E> type, byte enumValue) {
		Method parseMethod = null;
		try {
			parseMethod = type.getDeclaredMethod(ENUM_VALUE_PARSE_NAME);
		} catch (Exception e) {}
		if (null == parseMethod || !Modifier.isStatic(parseMethod.getModifiers())) return type.getEnumConstants()[enumValue];
		try {
			return (E) parseMethod.invoke(null, enumValue);
		} catch (Exception e) {
			throw new RuntimeException("Failure on parsing as enum.", e);
		}
	}
}
