package net.butfly.albacore.utils.collection;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.lambda.InvocationHandler;

public interface MapEx<K, V> extends Map<K, V> {
	static <K, V> MapEx<K, V> of(Map<K, V> origin) {
		return origin instanceof MapEx ? (MapEx<K, V>) origin : InvocationHandler.proxy((proxy, method, args) -> {
			return Context.MAPEX_METHODS.contains(method) ? method.invoke(proxy, args) : method.invoke(origin, args);
		}, MapEx.class);
	}

	default MapEx<K, V> add(K key, V value) {
		put(key, value);
		return this;
	}

	class Context {
		private static Set<Method> MAPEX_METHODS = new HashSet<>(Arrays.asList(MapEx.class.getDeclaredMethods()));
	}
}
