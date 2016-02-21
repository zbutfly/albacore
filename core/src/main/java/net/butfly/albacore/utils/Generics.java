package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.List;

import com.google.common.reflect.TypeToken;

@SuppressWarnings("unchecked")
public final class Generics extends Utils {
	public static <E> Class<E> resolveGenericParameter(final Type implType, final Class<?> declareClass,
			final String genericParamName) {
		TypeVariable<?>[] vv = declareClass.getTypeParameters();
		for (TypeVariable<?> v : vv) {
			if (genericParamName.equals(v.getName())) return (Class<E>) TypeToken.of(implType).resolveType(v).getRawType();
		}
		return null;
	}

	public static Class<?>[] resolveGenericParameters(final Type implType, final Class<?> declareClass) {
		List<Class<?>> types = Reflections.constructInterface(List.class);
		TypeVariable<?>[] vv = declareClass.getTypeParameters();
		for (TypeVariable<?> v : vv) {
			types.add((Class<?>) TypeToken.of(implType).resolveType(v).getRawType());
		}
		return toArray(types);
	}

	public static Class<?> resolveReturnType(final Type implType, Method method) {
		return TypeToken.of(implType).resolveType(method.getGenericReturnType()).getRawType();
	}

	public static Class<?> resolveFieldType(final Type type, Field field) {
		return TypeToken.of(type).resolveType(field.getGenericType()).getRawType();
	}

	public static <E> Type getGenericClass(E entity) {
		return TypeToken.of(entity.getClass()).getType();
	}

	public static <E> E[] toArray(Collection<E> list, Class<E> clazz) {
		if (null == list || list.isEmpty()) return (E[]) Array.newInstance(clazz, 0);
		return list.toArray((E[]) Array.newInstance(clazz, list.size()));
	}

	public static <E> E[] toArray(Collection<E> list) {
		if (null == list) throw new IllegalArgumentException("Could not determine class of element for null list argument.");
		Class<E> clazz = Generics.resolveGenericParameter(list.getClass(), Collection.class, "E");
		return toArray(list, clazz);
	}
}
