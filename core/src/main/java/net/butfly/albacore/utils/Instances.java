package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.google.common.reflect.TypeToken;

/**
 * Replacement of "private static final ..."
 * 
 * @author butfly
 * @see net.butfly.albacore.utils.collection.LayerMap<V>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class Instances extends Utils {
	private static final ConcurrentHashMap INSTANCE_POOL = new ConcurrentHashMap();

	public static <T> T construct(Class<T> constructClass, Object... constructParams) {
		return fetch(() -> (T) Reflections.construct(constructClass, constructParams), constructClass, constructParams);
	}

	public static <T> T fetch(Supplier<T> supplier, Class<T> cl, Object... key) {
		return null == supplier ? fetch(cl, key) : (T) fetchFrom(supplier, INSTANCE_POOL, cl, key, 0);
	}

	public static <T> T fetch(Supplier<T> supplier, TypeToken<T> cl, Object... key) {
		return fetch(supplier, (Class<T>) cl.getRawType(), key);
	}

	public static <T> T fetch(TypeToken<T> cl, Object... key) {
		return fetch((Class<T>) cl.getRawType(), key);
	}

	public static <T> T fetch(Class<T> cl, Object... key) {
		try {
			if (key == null || key.length == 0) return (T) INSTANCE_POOL.get(cl);
			Map curr = (Map) INSTANCE_POOL.get(cl);
			for (int i = 0; i < key.length; i++) {
				Object k = null == key[i] ? "" : key[i];
				if (i == key.length - 1) return (T) curr.get(k);
				curr = (Map) curr.get(k);
			}
			return null;
		} catch (Throwable th) {
			return null;
		}
	}

	private static <T> T fetchFrom(Supplier<T> supplier, Map pool, Object currKey, Object[] allKeys, int pos) {
		boolean bottom = allKeys.length <= pos;
		currKey = null == currKey ? "" : currKey;
		if (bottom) return (T) pool.computeIfAbsent(currKey, k -> supplier.get());
		else {
			Map subpool = (Map) pool.computeIfAbsent(currKey, k -> new ConcurrentHashMap());
			return fetchFrom(supplier, subpool, allKeys[pos], allKeys, pos + 1);
		}
	}
}
