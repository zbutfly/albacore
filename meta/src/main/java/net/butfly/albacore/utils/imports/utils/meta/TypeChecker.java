package net.butfly.albacore.utils.imports.utils.meta;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.support.Beans;
import net.butfly.albacore.utils.Generics;

final class TypeChecker {
	public static PrimaryCategory getPrimaryCategory(Class<?> clazz) {
		if (Enum.class.isAssignableFrom(clazz)) return PrimaryCategory.ENUM;
		if (boolean.class.equals(clazz) || Boolean.class.equals(clazz)) return PrimaryCategory.BOOL;
		if (NumberCategory.isNumber(clazz)) return PrimaryCategory.NUMBER;
		if (String.class.equals(clazz) || char.class.equals(clazz) || Character.class.equals(clazz)) return PrimaryCategory.STRING;
		if (clazz.isArray() || Iterable.class.isAssignableFrom(clazz)) return PrimaryCategory.LIST;
		if (Map.class.isAssignableFrom(clazz) || Beans.class.isAssignableFrom(clazz)) return PrimaryCategory.MAP;
		return PrimaryCategory.RAW_OBJ;
	}

	static Class<?> getIterableClass(Class<?> clazz) {
		if (clazz.isArray()) return clazz.getComponentType();
		if (Iterable.class.isAssignableFrom(clazz)) {
			Class<?> cl = Generics.resolveGenericParameter(clazz, Iterable.class, "T");
			return null == cl ? Object.class : cl;
		}
		return null;
	}

	private static final Set<Class<?>> ALL_NUMBER_CLASSES = new HashSet<Class<?>>();

	enum NumberCategory {
		INT(int.class, Integer.class), LONG(long.class, Long.class), BYTE(byte.class, Byte.class), SHORT(short.class, Short.class), FLOAT(
				float.class, Float.class), DOUBLE(double.class, Double.class), NUMBER(null, null);

		final Class<?> primitiveClass;
		final Class<? extends Number> boxedClass;

		NumberCategory(Class<?> primitiveClass, Class<? extends Number> boxedClass) {
			this.primitiveClass = primitiveClass;
			this.boxedClass = boxedClass;
			if (primitiveClass != null) ALL_NUMBER_CLASSES.add(primitiveClass);
			if (boxedClass != null) ALL_NUMBER_CLASSES.add(boxedClass);
		}

		static boolean isNumber(Class<?> clazz) {
			return ALL_NUMBER_CLASSES.contains(clazz);
		}

		static NumberCategory classify(Class<?> clazz) {
			for (NumberCategory cat : NumberCategory.values())
				if (cat.primitiveClass.equals(clazz) || cat.boxedClass.isAssignableFrom(clazz)) return cat;
			return null;
		}
	}

	enum PrimaryCategory {
		RAW_OBJ, STRING, NUMBER, BOOL, LIST, MAP, ENUM
	}
}