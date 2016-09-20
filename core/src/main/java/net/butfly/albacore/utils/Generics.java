package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;

@SuppressWarnings("unchecked")
public final class Generics extends Utils {
	public static Class<?> resolveReturnType(final Type implType, Method method) {
		return TypeToken.of(implType).resolveType(method.getGenericReturnType()).getRawType();
	}

	public static Class<?> resolveFieldType(final Type type, Field field) {
		return TypeToken.of(type).resolveType(field.getGenericType()).getRawType();
	}

	public static <E> Class<E> getGenericClass(E... entity) {
		return (Class<E>) TypeToken.of(entity.getClass().getComponentType()).getType();
	}

	public static Map<String, Class<?>> resolveGenericParameters(final Type implType, final Class<?> declareClass) {
		return Instances.fetch(() -> {
			Map<String, Class<?>> types = Reflections.constructInterface(Map.class);
			TypeVariable<?>[] vv = declareClass.getTypeParameters();
			for (TypeVariable<?> v : vv) {
				types.put(v.getName(), (Class<?>) TypeToken.of(implType).resolveType(v).getRawType());
			} ;
			return types;
		}, implType, declareClass);
	}

	public static <E> Class<E> resolveGenericParameter(final Type implType, final Class<?> declareClass, final String genericParamName) {
		return (Class<E>) resolveGenericParameters(implType, declareClass).get(genericParamName);
	}

	public static <E> E[] toArray(Collection<E> list) {
		if (null == list) throw new IllegalArgumentException("Could not determine class of element for null list argument.");
		Class<E> clazz = resolveGenericParameter(list.getClass(), Collection.class, "E");
		return toArray(list, clazz);
	}

	public static <E> E[] toArray(Collection<E> list, Class<E> clazz) {
		if (null == list || list.isEmpty()) return (E[]) Array.newInstance(clazz, 0);
		return list.toArray((E[]) Array.newInstance(clazz, list.size()));
	}

	@SafeVarargs
	public static <T> T[] array(T... elems) {
		return elems;
	}

	public static <T> T[] array(T elem, int len) {
		List<T> l = new ArrayList<T>(len);
		for (int i = 0; i < len; i++)
			l.add(elem);
		return l.toArray(array());
	}
}
