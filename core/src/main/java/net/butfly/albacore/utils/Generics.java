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
import java.util.concurrent.ConcurrentMap;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

@SuppressWarnings("unchecked")
public final class Generics extends OldGeneric {
	public static Class<?> resolveReturnType(final Type implType, Method method) {
		return TypeToken.of(implType).resolveType(method.getGenericReturnType()).getRawType();
	}

	public static Class<?> resolveFieldType(final Type type, Field field) {
		return TypeToken.of(type).resolveType(field.getGenericType()).getRawType();
	}

	public static <E> Class<E> getGenericClass(E... entity) {
		return (Class<E>) TypeToken.of(entity.getClass().getComponentType()).getType();
	}

	public static <E> Class<E> getGenericClass(E entity) {
		return (Class<E>) TypeToken.of(entity.getClass()).getType();
	}

	static final Map<Type, Map<Class<?>, Map<String, Class<?>>>> pools = Maps.of();

	public static Map<String, Class<?>> resolveGenericParameters(final Type implType, final Class<?> declareClass) {
		return pools.computeIfAbsent(implType, tt -> Maps.of()).computeIfAbsent(declareClass, cc -> compute(implType, declareClass));
	}

	private static final Map<String, Class<?>> compute(final Type implType, final Class<?> declareClass) {
		ConcurrentMap<String, Class<?>> types = Maps.of();
		TypeVariable<?>[] vv = declareClass.getTypeParameters();
		for (TypeVariable<?> v : vv)
			types.put(v.getName(), (Class<?>) TypeToken.of(implType).resolveType(v).getRawType());
		return types;
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
		if (Colls.empty(list)) return (E[]) Array.newInstance(clazz, 0);
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

	public static <T> Type type(Class<T> generic, Class<?>... param) {
		return sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl.make(generic, param, null);
	}

	public static TypeToken<?>[] tokens(Class<?>... parameterClasses) {
		TypeToken<?>[] ts = new TypeToken[parameterClasses.length];
		for (int i = 0; i < parameterClasses.length; i++)
			ts[i] = null == parameterClasses[i] ? null : TypeToken.of(parameterClasses[i]);
		return ts;
	}
}
