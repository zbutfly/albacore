package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

@SuppressWarnings("unchecked")
public final class Generics extends Utils {
	public static Class<?> getSuperClassGenricType(Class<?> clazz, int index) {
		Type genType = clazz.getGenericSuperclass();// 得到泛型父类
		if (null == genType) { throw new RuntimeException("Counld not found the generic parameter in super classes!"); }
		if (genType instanceof ParameterizedType) {
			Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
			if (index < params.length && index >= 0) {
				return (Class<?>) params[index];
			} else {
				throw new RuntimeException("Index of parameter out of bound!");
			}
		}
		return getSuperClassGenricType((Class<?>) genType, index);
	}

	public static <E> Class<E> getGenericParamClass(final Class<?> childClass, final Class<?> parentClass,
			final String paramName) {
		return Instances.fetch(new Callable<Class<E>>() {
			@Override
			public Class<E> call() {
				Map<TypeVariable<Class<?>>, Type> map = getTypeVariableMap(childClass);
				for (TypeVariable<?> v : map.keySet())
					if (parentClass.equals(v.getGenericDeclaration()) && paramName.equals(v.getName()))
						return (Class<E>) map.get(v);
				return null;
			}
		}, parentClass, childClass, paramName);
	}

	public static Class<?> resolveReturnType(Method method, Class<?> clazz) {
		Type genericType = method.getGenericReturnType();
		Map<TypeVariable<Class<?>>, Type> typeVariableMap = getTypeVariableMap(clazz);
		Type rawType = getRawType(genericType, typeVariableMap);
		return (rawType instanceof Class<?>) ? (Class<?>) rawType : method.getReturnType();
	}

	public static Class<?> resolveType(Type genericType, Map<TypeVariable<Class<?>>, Type> typeVariableMap) {
		Type rawType = getRawType(genericType, typeVariableMap);
		return (rawType instanceof Class<?>) ? (Class<?>) rawType : java.lang.Object.class;
	}

	private static Type extractBoundForTypeVariable(TypeVariable<Class<?>> typeVariable) {
		Type bounds[] = typeVariable.getBounds();
		if (bounds.length == 0) { return java.lang.Object.class; }
		Type bound = bounds[0];
		if (bound instanceof TypeVariable<?>) {
			bound = extractBoundForTypeVariable((TypeVariable<Class<?>>) bound);
		}
		return bound;
	}

	private static void extractTypeVariablesFromGenericInterfaces(Type genericInterfaces[],
			Map<TypeVariable<Class<?>>, Type> typeVariableMap) {
		for (int i = 0; i < genericInterfaces.length; i++) {
			Type genericInterface = genericInterfaces[i];
			if (genericInterface instanceof ParameterizedType) {
				ParameterizedType pt = (ParameterizedType) genericInterface;
				populateTypeMapFromParameterizedType(pt, typeVariableMap);
				if (pt.getRawType() instanceof Class<?>) {
					extractTypeVariablesFromGenericInterfaces(((Class<?>) pt.getRawType()).getGenericInterfaces(),
							typeVariableMap);
				}
				continue;
			}
			if (genericInterface instanceof Class<?>) {
				extractTypeVariablesFromGenericInterfaces(((Class<?>) genericInterface).getGenericInterfaces(), typeVariableMap);
			}
		}

	}

	private static Type getRawType(Type genericType, Map<TypeVariable<Class<?>>, Type> typeVariableMap) {
		Type resolvedType = genericType;
		if (genericType instanceof TypeVariable<?>) {
			TypeVariable<Class<?>> tv = (TypeVariable<Class<?>>) genericType;
			resolvedType = (Type) typeVariableMap.get(tv);
			if (resolvedType == null) {
				resolvedType = extractBoundForTypeVariable(tv);
			}
		}
		if (resolvedType instanceof ParameterizedType) {
			return ((ParameterizedType) resolvedType).getRawType();
		} else {
			return resolvedType;
		}
	}

	private static void populateTypeMapFromParameterizedType(ParameterizedType type,
			Map<TypeVariable<Class<?>>, Type> typeVariableMap) {
		if (type.getRawType() instanceof Class<?>) {
			Type actualTypeArguments[] = type.getActualTypeArguments();
			@SuppressWarnings("rawtypes")
			TypeVariable<Class<?>> typeVariables[] = ((Class) type.getRawType()).getTypeParameters();
			for (int i = 0; i < actualTypeArguments.length; i++) {
				Type actualTypeArgument = actualTypeArguments[i];
				TypeVariable<Class<?>> variable = typeVariables[i];
				if (actualTypeArgument instanceof Class) {
					typeVariableMap.put(variable, actualTypeArgument);
					continue;
				}
				if (actualTypeArgument instanceof GenericArrayType) {
					typeVariableMap.put(variable, actualTypeArgument);
					continue;
				}
				if (actualTypeArgument instanceof ParameterizedType) {
					typeVariableMap.put(variable, actualTypeArgument);
					continue;
				}
				if (!(actualTypeArgument instanceof TypeVariable<?>)) {
					continue;
				}
				TypeVariable<Class<?>> typeVariableArgument = (TypeVariable<Class<?>>) actualTypeArgument;
				Type resolvedType = (Type) typeVariableMap.get(typeVariableArgument);
				if (resolvedType == null) {
					resolvedType = extractBoundForTypeVariable(typeVariableArgument);
				}
				typeVariableMap.put(variable, resolvedType);
			}

		}
	}

	private static Map<TypeVariable<Class<?>>, Type> getTypeVariableMap(final Class<?> clazz) {
		return Instances.fetch(new Callable<Map<TypeVariable<Class<?>>, Type>>() {
			@Override
			public Map<TypeVariable<Class<?>>, Type> call() {
				Map<TypeVariable<Class<?>>, Type> typeVariableMap = new HashMap<TypeVariable<Class<?>>, Type>();
				extractTypeVariablesFromGenericInterfaces(clazz.getGenericInterfaces(), typeVariableMap);
				Type genericType = clazz.getGenericSuperclass();
				for (Class<?> type = clazz.getSuperclass(); type != null && !(java.lang.Object.class).equals(type); type = type
						.getSuperclass()) {
					if (genericType instanceof ParameterizedType) {
						ParameterizedType pt = (ParameterizedType) genericType;
						populateTypeMapFromParameterizedType(pt, typeVariableMap);
					}
					extractTypeVariablesFromGenericInterfaces(type.getGenericInterfaces(), typeVariableMap);
					genericType = type.getGenericSuperclass();
				}
				for (Class<?> type = clazz; type.isMemberClass(); type = type.getEnclosingClass()) {
					genericType = type.getGenericSuperclass();
					if (genericType instanceof ParameterizedType) {
						ParameterizedType pt = (ParameterizedType) genericType;
						populateTypeMapFromParameterizedType(pt, typeVariableMap);
					}
				}
				return typeVariableMap;
			}
		}, clazz);
	}

	public static Field getDeclaredField(Class<?> clazz, String name) {
		while (null != clazz) {
			try {
				return clazz.getDeclaredField(name);
			} catch (NoSuchFieldException ex) {}
			clazz = clazz.getSuperclass();
		}
		return null;
	}

	public static Field[] getDeclaredFields(Class<?> clazz) {
		Set<Field> set = new HashSet<Field>();
		while (null != clazz) {
			set.addAll(Arrays.asList(clazz.getDeclaredFields()));
			clazz = clazz.getSuperclass();
		}
		return set.toArray(new Field[set.size()]);
	}

	public static <E> Class<E> entityClass(E entity) {
		return (Class<E>) entity.getClass();
	}

	public static <E> Class<E> entityClass(E... entity) {
		return (Class<E>) entity.getClass().getComponentType();
	}

	public static <E> E[] toArray(List<E> list, Class<E> clazz) {
		if (null == list) return (E[]) Array.newInstance(clazz, 0);
		return list.toArray((E[]) Array.newInstance(clazz, list.size()));
	}
}
