package net.butfly.albacore.utils.collection;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.lambda.InvocationHandler;

public abstract class MapEx<K, V> implements Map<K, V> {
	public static <K, V> MapEx<K, V> of(final Map<K, V> origin) {
		if (origin instanceof MapEx) return (MapEx<K, V>) origin;
		else {
			@SuppressWarnings("unchecked")
			MapEx<K, V> map = InvocationHandler.proxy(new InvocationHandler() {
				@Override
				public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
					return MAPEX_METHODS.contains(method) ? method.invoke(proxy, args) : method.invoke(origin, args);
				}
			}, MapEx.class);
			return map;
		}
	}

	public MapEx<K, V> add(K key, V value) {
		put(key, value);
		return this;
	}

	private static final Set<Method> MAPEX_METHODS = new HashSet<Method>(Arrays.asList(MapEx.class.getDeclaredMethods()));
}
